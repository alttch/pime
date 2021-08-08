# Rust Python Integration Made Easy

## What is this for?

PIME is a Rust crate, which allows [tokio](https://tokio.rs)-based Rust
programs to easily execute Python code snippets.

PIME is based on [PyO3](https://pyo3.rs/) and
[neotasker](https://pypi.org/project/neotasker/) Python module.

PIME allows Rust to execute Python blocking code in parallel, using standard
ThreadPoolExecutor and await for results of called concurrent.futures objects.

PIME is absolutely thread-safe and has got a goal to make Python integration
into Rust as simple as possible.

## How does it work

Let us look inside PIME-integrated Rust program:

|Rust thread | Rust thread | Rust thread | Python GIL thread  |
|------------|-------------|-------------|--------------------|
|rust code   | rust code   | rust code   | ThreadPoolExecutor |
|rust code   | await task1 | rust code   | task1              |
|await task2 | await task3 | rust code   | task2 task3        |
