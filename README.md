# Rust Python Integration Made Easy

## What is this for

PIME is a Rust crate, which allows [tokio](https://tokio.rs)-based Rust
programs to easily execute Python code snippets.

PIME is based on [PyO3](https://pyo3.rs/), [Serde](https://serde.rs) and
[neotasker](https://pypi.org/project/neotasker/) Python module.

PIME allows Rust to execute Python blocking code in parallel, using standard
ThreadPoolExecutor and await for results of called concurrent.futures objects.

PIME is absolutely thread-safe and has got a goal to make Python integration
into Rust as simple as possible.

PIME allows Rust programs to have Python-based extensions, plugins, integration
of Python scripts directly into Rust web servers etc.

## How does it work

Let us look inside PIME-integrated Rust program:

|Rust thread | Rust thread | Rust thread | Python GIL thread  |
|------------|-------------|-------------|--------------------|
|rust code   | rust code   | rust code   | ThreadPoolExecutor |
|rust code   | await task1 | rust code   | task1              |
|await task2 | await task3 | rust code   | task2 task3        |

When a Rust coroutine wants to execute a Python task, it creates
**pime::PyTask** object and executes **pime::call** method. If the execution is
successful, the result is returned as
[serde-value::Value](https://crates.io/crates/serde-value) object, otherwise as
**pime::Error**, which contains either Python exception information or an
engine error.

On the Python side, all tasks are handled by the broker function. The function
has two arguments: *command* and *params* (no keyword-based arguments, sorry -
current limitation of the asyncio loops' *run\_in\_executor* function). Broker
function instances are launched in parallel, using Python's ThreadPoolExecutor.

When the broker returns a result or raises an exception, this is reported back
to the Rust code.

Communication is performed via thread-safe mpsc channels.

## Usage example

### Preparing

Install neotasker module for Python:

```shell
pip3 install neotasker
```

### Cargo dependencies

```toml
[dependencies]
tokio = { version = "1.4", features = ["full"] }
pyo3 = { version = "0.14.1" }
serde-value = "0.7.0"
pime = "*"
```

### Rust code

```rust,ignore
use pyo3::prelude::*;
use serde_value::Value;
use std::collections::BTreeMap;
use std::env;

// create tokio runtime or use #[tokio::main]
// ...............................................
// ...............................................


// init and start PIME
tokio::task::spawn_blocking(move || {
    // omit if auto-prepared
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        // as Python has GIL,
        // all work with the Python object MUST be performed in this thread
        // after there is no way to reconfigure it
        let engine = pime::PySyncEngine::new(&py).unwrap();
        // inserts directories into Python's sys.path
        let cwd = env::current_dir().unwrap().to_str().unwrap().to_owned();
        engine.add_import_path(&cwd).unwrap();
        // enables debug mode
        engine.enable_debug().unwrap();
        // sets ThreadPoolExecutor size to min = 10, max = 10
        engine.set_thread_pool_size(10, 10).unwrap();
        let module = py.import("mymod").unwrap();
        let broker = module.getattr("broker").unwrap();
        // Perform additional work, e.g. add Rust functions to Python modules
        // .................................
        // fire and go
        engine.launch(&py, broker).unwrap();
    });
});
// wait engine to be started
pime::wait_online().await;

// Done! Now tasks can be called from any coroutine
// ...............................................
// ...............................................

let mut params = BTreeMap::new();
params.insert("name".to_owned(), Value::String("Rust".to_owned()));
let mut task = pime::PyTask::new(Value::String("hello".to_owned()), params);
// If the task result is not required, the task can be marked to be executed
// forever in ThreadPoolExecutor, until finished. In this case, "call" always
// returns result None
//task.no_wait();
// If a task performs calculations only, it can be marked as exclusive.
// Tasks of this type lock Python thread until completed. Use with care!
//task.mark_exclusive();
match pime::call(task).await {
    Ok(result) => {
        // The result is returned as Option<Value>
        println!("{:?}", result);
    },
    Err(e) if e.kind == pime::ErrorKind::PyException => {
        println!("Exception raised {}: {}", e.exception.unwrap(), e.message);
        println!("{}", e.traceback.unwrap());
    }
    Err(e) => {
        println!("An error is occurred: {}", e.message);
    }
};
// stop the engine gracefully
pime::stop().await;
```

### Python code (mymod/\_\_init\_\_.py)

```python
def broker(command, params):
    if command == 'hello':
        return f'Hi from Python, {params["name"]}!'
    elif command == 'bye':
        return 'Bye bye'
    else:
        raise RuntimeError('command unsupported')
```

## More examples

https://github.com/alttch/pime/tree/main/examples/

