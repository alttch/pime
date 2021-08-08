//! # Rust Python Integration Made Easy
//! 
//! ## What is this for?
//! 
//! PIME is a Rust crate, which allows [tokio](https://tokio.rs)-based Rust
//! programs to easily execute Python code snippets.
//! 
//! PIME is based on [PyO3](https://pyo3.rs/), [Serde](https://serde.rs) and
//! [neotasker](https://pypi.org/project/neotasker/) Python module.
//! 
//! PIME allows Rust to execute Python blocking code in parallel, using standard
//! ThreadPoolExecutor and await for results of called concurrent.futures objects.
//! 
//! PIME is absolutely thread-safe and has got a goal to make Python integration
//! into Rust as simple as possible.
//! 
//! PIME allows Rust programs to have Python-based extensions, plugins, integration
//! of Python scripts directly into Rust web servers etc.
//! 
//! ## How does it work
//! 
//! Let us look inside PIME-integrated Rust program:
//! 
//! |Rust thread | Rust thread | Rust thread | Python GIL thread  |
//! |------------|-------------|-------------|--------------------|
//! |rust code   | rust code   | rust code   | ThreadPoolExecutor |
//! |rust code   | await task1 | rust code   | task1              |
//! |await task2 | await task3 | rust code   | task2 task3        |
//! 
//! When a Rust coroutine want to execute a Python task, it creates
//! **pime::PyTask** object and executes **pime::call** method. If the execution is
//! successful, the result is returned as
//! (serde-value::Value)[https://crates.io/crates/serde-value] object, otherwise as
//! **pime::Error**, which contains either Python exception information or an
//! engine error.
//! 
//! On the Python side, all tasks are handled by the broker function. The function
//! has two arguments: *command* and *params* (no keyword-based arguments, sorry -
//! current limitation of the asyncio loops' *run\_in\_executor* function). Broker
//! function instances are launched in parallel, using Python's ThreadPoolExecutor.
//! 
//! When the broker returns a result or raises an exception, this is reported back
//! to the Rust code.
//! 
//! Communication is performed via thread-safe mpsc channels.
//! 
//! ## Usage example
//! 
//! ### Preparing
//! 
//! Install neotasker module for Python:
//! 
//! ```shell
//! pip3 install neotasker
//! ```
//! 
//! ### Cargo dependencies
//! 
//! ```toml
//! [dependencies]
//! tokio = { version = "1.4", features = ["full"] }
//! pyo3 = { version = "0.14.1", features = [] }
//! serde-value = "0.7.0"
//! pime = { path = "/opt/pime" }
//! ```
//! 
//! ### Rust code
//! 
//! ```rust,ignore
//! use pyo3::prelude::*;
//! use serde_value::Value;
//! use std::collections::BTreeMap;
//! use std::env;
//! 
//! // create tokio runtime or use #[tokio::main]
//! // ...............................................
//! // ...............................................
//! 
//! 
//! // init and start PIME
//! tokio::task::spawn_blocking(move || {
//!     // omit if auto-prepared
//!     pyo3::prepare_freethreaded_python();
//!     Python::with_gil(|py| {
//!         // as Python has GIL,
//!         // all work with the Python object MUST be performed in this thread
//!         // after there is no way to reconfigure it
//!         let engine = pime::PySyncEngine::new(&py).unwrap();
//!         let cwd = env::current_dir().unwrap().to_str().unwrap().to_owned();
//!         // inserts directories into Python's sys.path
//!         engine.add_import_path(&cwd).unwrap();
//!         // enables debug mode
//!         engine.enable_debug().unwrap();
//!         // sets ThreadPoolExecutor size to min = 10, max = 10
//!         engine.set_thread_pool_size(10, 10).unwrap();
//!         let module = py.import("mymod").unwrap();
//!         let broker = module.getattr("broker").unwrap();
//!         // Perform additional work, e.g. add Rust functions to Python modules
//!         // .................................
//!         // fire and go
//!         engine.launch(&py, broker).unwrap();
//!     });
//! });
//! // wait runtime to be started
//! pime::wait_online();
//! 
//! // spawn example tasks from any coroutine
//! // ...............................................
//! // ...............................................
//! 
//! let mut params = BTreeMap::new();
//! params.insert("name".to_owned(), Value::String("Rust".to_owned()));
//! let mut task = pime::PyTask::new(Value::String("hello".to_owned()), params);
//! // if the task result is not required, the task can be marked to be executed
//! // forever in ThreadPoolExecutor, until finished. In this case, await always
//! // returns None
//! //task.no_wait();
//! match pime::call(task).await {
//!     Ok(result) => {
//!         // The result is returned as Option<Value>
//!         println!("{:?}", result);
//!     },
//!     Err(e) if e.kind == pime::ErrorKind::PythonError => {
//!         println!("Exception raised {}: {}", e.exception.unwrap(), e.message);
//!         println!("{}", e.traceback.unwrap());
//!     }
//!     Err(e) => {
//!         println!("An error is occurred: {}", e.message);
//!     }
//! };
//! // stop the engine gracefully
//! pime::stop().await;
//! ```
//! 
//! ### Python code (mymod/\_\_init\_\_.py)
//! 
//! ```python
//! def broker(command, params):
//!     if command == 'hello':
//!         return f'Hi from Python, {params["name"]}!'
//!     elif command == 'bye':
//!         return 'Bye bye'
//!     else:
//!         raise RuntimeError('command unsupported')
//! ```
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

#[derive(Debug, Eq, PartialEq)]
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
    pub kind: ErrorKind,
    pub message: String,
    pub exception: Option<String>,
    pub traceback: Option<String>,
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
    command: Value,
    params: BTreeMap<String, Value>,
    need_result: bool,
}

impl PyTask {
    pub fn new(command: Value, params: BTreeMap<String, Value>) -> Box<Self> {
        Box::new(Self {
            command,
            params,
            need_result: true,
        })
    }

    pub fn new0(command: Value) -> Box<Self> {
        Box::new(Self {
            command,
            params: BTreeMap::new(),
            need_result: true,
        })
    }

    pub fn no_wait(&mut self) {
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
                    let command = pythonize(*py, &task.command);
                    let params = pythonize(*py, &task.params);
                    if command.is_err() {
                        if task_id != 0 {
                            report_error(
                                task_id,
                                Error::new(ErrorKind::PackError, tostr!(params.err().unwrap())),
                            );
                        }
                        continue;
                    }
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
                        match call.call1((task_id, exec_func, command.unwrap(), params.unwrap())) {
                            Ok(_) => {}
                            Err(e) => {
                                report_error(task_id, Error::new(ErrorKind::ExecError, tostr!(e)));
                            }
                        }
                    } else {
                        let _ = spawn.call1((exec_func, command.unwrap(), params.unwrap()));
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
