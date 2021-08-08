use log::{debug, error};
use pyo3::prelude::*;
use pythonize::{depythonize, pythonize};
use serde_value::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};

pub const STATE_STOPPED: u8 = 0;
pub const STATE_STARTING: u8 = 1;
pub const STATE_STOPPING: u8 = 2;
pub const STATE_STARTED: u8 = 0xff;

use std::sync::atomic::{AtomicU8, Ordering};

#[macro_use]
extern crate lazy_static;

macro_rules! tostr {
    ($s: expr) => {
        format!("{}", $s)
    };
}

static ENGINE_STATE: AtomicU8 = AtomicU8::new(STATE_STOPPED);

#[derive(Debug)]
pub enum ErrorKind {
    PythonError,
    PackError,
    UnpackError,
    ExecError,
    InternalError,
    PySyncEngineStateError,
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    exception: Option<String>,
    traceback: Option<String>,
}

impl From<PyErr> for Box<Error> {
    fn from(e: PyErr) -> Box<Error> {
        Error::new(ErrorKind::InternalError, tostr!(e))
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Box<Error>
where
    T: std::fmt::Debug,
{
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Box<Error> {
        Error::new_internal(tostr!(e))
    }
}

impl From<tokio::sync::TryLockError> for Box<Error> {
    fn from(e: tokio::sync::TryLockError) -> Box<Error> {
        Error::new_internal(tostr!(e))
    }
}

impl Error {
    pub fn new(kind: ErrorKind, message: String) -> Box<Self> {
        Box::new(Self {
            kind,
            message: message.to_owned(),
            exception: None,
            traceback: None,
        })
    }
    pub fn new_py(error: (String, String, String)) -> Box<Self> {
        Box::new(Self {
            kind: ErrorKind::PythonError,
            exception: Some(error.0),
            message: error.1,
            traceback: Some(error.2),
        })
    }

    pub fn new_internal(message: String) -> Box<Self> {
        Box::new(Self {
            kind: ErrorKind::PySyncEngineStateError,
            message: format!("CRITICAL: PySyncEngine internal error: {}", message),
            exception: None,
            traceback: None,
        })
    }

    pub fn new_offline() -> Self {
        Self {
            kind: ErrorKind::PySyncEngineStateError,
            message: "PySyncEngine is offline".to_owned(),
            exception: None,
            traceback: None,
        }
    }

    pub fn new_online() -> Self {
        Self {
            kind: ErrorKind::PySyncEngineStateError,
            message: "PySyncEngine is online".to_owned(),
            exception: None,
            traceback: None,
        }
    }
    pub fn new_not_ready() -> Self {
        Self {
            kind: ErrorKind::PySyncEngineStateError,
            message: "PySyncEngine is not initialized".to_owned(),
            exception: None,
            traceback: None,
        }
    }
}

#[derive(Debug)]
pub struct PyTask {
    func: String,
    params: BTreeMap<String, Value>,
    need_result: bool,
}

impl PyTask {
    pub fn new(func: String, params: BTreeMap<String, Value>) -> Box<Self> {
        Box::new(Self {
            func,
            params,
            need_result: true,
        })
    }

    pub fn new0(func: String) -> Box<Self> {
        Box::new(Self {
            func,
            params: BTreeMap::new(),
            need_result: true,
        })
    }

    pub fn no_wait_result(&mut self) {
        self.need_result = false;
    }
}

struct DataChannel {
    tx: Mutex<mpsc::Sender<(u64, Option<Box<PyTask>>)>>,
    rx: Mutex<mpsc::Receiver<(u64, Option<Box<PyTask>>)>>,
}

impl DataChannel {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<(u64, Option<Box<PyTask>>)>(1024);
        Self {
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
        }
    }
}

#[derive(Debug)]
struct PyTaskResult {
    task_id: u64,
    ready: Arc<Notify>,
    result: Option<Box<Value>>,
    error: Option<Box<Error>>,
}

impl PyTaskResult {
    fn set_result(&mut self, result: Option<Value>) {
        match result {
            Some(v) => self.result = Some(Box::new(v)),
            None => self.result = None,
        }
    }
    fn set_error(&mut self, error: Box<Error>) {
        self.error = Some(error);
    }
}

struct PyTaskCounter {
    id: u64,
}

impl PyTaskCounter {
    fn new() -> Self {
        Self { id: 0 }
    }
    fn get(&mut self) -> u64 {
        if self.id == std::u64::MAX {
            self.id = 1;
        } else {
            self.id += 1;
        }
        self.id
    }
}

lazy_static! {
    static ref PY_RESULTS: RwLock<Box<BTreeMap<u64, PyTaskResult>>> =
        RwLock::new(Box::new(BTreeMap::new()));
    static ref TASK_COUNTER: Mutex<PyTaskCounter> = Mutex::new(PyTaskCounter::new());
    static ref DC: DataChannel = DataChannel::new();
}

pub struct PySyncEngine<'p> {
    neo: &'p pyo3::types::PyModule,
}

macro_rules! need_online {
    () => {
        if ENGINE_STATE.load(Ordering::SeqCst) != STATE_STARTED {
            return Err(Box::new(Error::new_offline()));
        }
    };
}

macro_rules! need_offline {
    () => {
        if ENGINE_STATE.load(Ordering::SeqCst) != STATE_STOPPED {
            return Err(Box::new(Error::new_online()));
        }
    };
}

macro_rules! critical {
    ($msg: expr) => {
        error!("PySyncEngine CRIICAL: {}", $msg);
    };
}

macro_rules! log_lost_task {
    ($task_id: expr) => {
        error!("PySyncEngine CRIICAL: task {} is lost", $task_id);
    };
}

fn report_error(task_id: u64, error: Box<Error>) {
    loop {
        match PY_RESULTS.try_write() {
            Ok(mut v) => {
                let o = match v.get_mut(&task_id) {
                    Some(x) => x,
                    None => {
                        log_lost_task!(task_id);
                        return;
                    }
                };
                o.set_error(error);
                o.ready.notify_waiters();
                break;
            }
            Err(_) => {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
        }
    }
}

impl<'p> PySyncEngine<'p> {
    pub fn new(py: &'p pyo3::Python) -> Result<Self, Box<Error>> {
        if ENGINE_STATE.load(Ordering::SeqCst) != STATE_STOPPED {
            return Err(Box::new(Error::new_online()));
        }
        #[pyfunction]
        fn report_result(
            py: Python,
            task_id: u64,
            result: Option<Py<PyAny>>,
            error: Option<(String, String, String)>,
        ) {
            let data: Option<Value> = match result {
                Some(r) => match depythonize(r.as_ref(py)) {
                    Ok(v) => v,
                    Err(e) => {
                        report_error(task_id, Error::new(ErrorKind::UnpackError, tostr!(e)));
                        return;
                    }
                },
                None => None,
            };
            loop {
                match PY_RESULTS.try_write() {
                    Ok(mut v) => {
                        let o = match v.get_mut(&task_id) {
                            Some(x) => x,
                            None => {
                                log_lost_task!(task_id);
                                return;
                            }
                        };
                        o.set_result(data);
                        match error {
                            Some(e) => o.set_error(Error::new_py(e)),
                            None => {}
                        }
                        o.ready.notify_waiters();
                        break;
                    }
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                }
            }
        }
        let neo = py.import("neotasker.embed")?;
        neo.add_function(wrap_pyfunction!(report_result, neo)?)?;
        Ok(Self { neo })
    }

    pub fn add_import_path(&self, path: &str) -> Result<(), Box<Error>> {
        match self.neo.call_method1("add_import_path", (path,)) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::new(ErrorKind::PythonError, tostr!(e))),
        }
    }

    pub fn enable_debug(&self) -> Result<(), Box<Error>> {
        self.neo.call_method0("set_debug")?;
        Ok(())
    }

    pub fn set_poll_delay(&self, delay: f32) -> Result<(), Box<Error>> {
        self.neo.call_method1("set_poll_delay", (delay,))?;
        Ok(())
    }

    pub fn set_thread_pool_size(&self, min: u32, max: u32) -> Result<(), Box<Error>> {
        self.neo
            .call_method1("set_thread_pool_size", ((min, max),))?;
        Ok(())
    }

    pub fn launch(&self, py: &'p pyo3::Python, exec_func: &pyo3::PyAny) -> Result<(), Box<Error>> {
        need_offline!();

        let mut rx = DC.rx.try_lock()?;

        self.neo.call_method0("start")?;
        let call = self.neo.getattr("call")?;
        let spawn = self.neo.getattr("spawn")?;

        ENGINE_STATE.store(STATE_STARTED, Ordering::SeqCst);
        debug!("PySyncEngine started");
        loop {
            let (task_id, t) = match py.allow_threads(|| rx.blocking_recv()) {
                Some(v) => v,
                None => {
                    return Err(Error::new_internal("channel broken".to_owned()));
                }
            };
            match t {
                None => {
                    ENGINE_STATE.store(STATE_STOPPING, Ordering::SeqCst);
                    break;
                }
                Some(task) => {
                    let params = pythonize(*py, &task.params);
                    if params.is_err() {
                        if task_id != 0 {
                            report_error(
                                task_id,
                                Error::new(ErrorKind::PackError, tostr!(params.err().unwrap())),
                            );
                        }
                        continue;
                    }
                    if task.need_result {
                        match call.call1((task_id, exec_func, &task.func, params.unwrap())) {
                            Ok(_) => {}
                            Err(e) => {
                                report_error(task_id, Error::new(ErrorKind::ExecError, tostr!(e)));
                            }
                        }
                    } else {
                        let _ = spawn.call1((exec_func, &task.func, params.unwrap()));
                    }
                }
            }
        }
        debug!("Stopping PySyncEngine");
        self.neo.call_method0("stop")?;
        debug!("PySyncEngine stopped");
        ENGINE_STATE.store(STATE_STOPPED, Ordering::SeqCst);
        Ok(())
    }
}

pub async fn call(task: Box<PyTask>) -> Result<Option<Box<Value>>, Box<Error>> {
    need_online!();
    if !task.need_result {
        DC.tx.lock().await.send((0, Some(task))).await?;
        return Ok(None);
    }
    let task_id;
    loop {
        let cid = TASK_COUNTER.lock().await.get();
        match PY_RESULTS.read().await.get(&cid) {
            Some(_) => critical!("dead tasks in result map"),
            None => {
                task_id = cid;
                break;
            }
        };
    }
    let beacon = Arc::new(Notify::new());
    PY_RESULTS.write().await.insert(
        task_id,
        PyTaskResult {
            task_id,
            result: None,
            error: None,
            ready: beacon.clone(),
        },
    );
    DC.tx.lock().await.send((task_id, Some(task))).await?;
    beacon.notified().await;
    let res = match PY_RESULTS.write().await.remove(&task_id) {
        Some(v) => v,
        None => {
            return Err(Error::new(
                ErrorKind::InternalError,
                "CRITICAL: Result not found, engine broken".to_owned(),
            ));
        }
    };
    match res.error {
        Some(e) => Err(e),
        None => Ok(res.result),
    }
}

pub async fn stop() -> Result<(), Box<Error>> {
    need_online!();
    DC.tx.lock().await.send((0, None)).await?;
    wait_offline();
    Ok(())
}

pub fn get_engine_state() -> u8 {
    ENGINE_STATE.load(Ordering::SeqCst)
}

pub fn is_engine_started() -> bool {
    ENGINE_STATE.load(Ordering::SeqCst) == STATE_STARTED
}

pub fn wait_online() {
    while ENGINE_STATE.load(Ordering::SeqCst) != STATE_STARTED {
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub fn wait_offline() {
    while ENGINE_STATE.load(Ordering::SeqCst) != STATE_STOPPED {
        std::thread::sleep(Duration::from_millis(10));
    }
}
