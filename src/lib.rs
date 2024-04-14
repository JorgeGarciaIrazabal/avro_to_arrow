use arrow_array::{Int32Array};
use arrow::{pyarrow::PyArrowType, record_batch::RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use std::sync::Arc;
mod avro_utils;
// mod build;

fn create_arrow_table() -> RecordBatch {
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false)
    ]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
    batch
}

use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
fn create_arrow_table_py() -> PyResult<PyArrowType<RecordBatch>> {
    let batch1 = create_arrow_table();
    let batch2 = create_arrow_table();
    Ok(PyArrowType(batch1))
}

struct RecordBachIterator {
    file: String,
    // record_batches is a list of record batches
    record_batches: Box<dyn Iterator<Item = RecordBatch> + Send>
}

impl RecordBachIterator {
    // Constructs a new instance of [`Second`].
    // Note this is an associated function - no self.
    pub fn new(file: String) -> Self {
        Self { file, record_batches: Box::new(vec![create_arrow_table(), create_arrow_table()].into_iter()) }
    }
}

impl Iterator for RecordBachIterator {
    // We can refer to this type using Self::Item
    type Item = PyArrowType<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        println!("next");
        let current = self.file.clone();

        // Since there's no endpoint to a Fibonacci sequence, the `Iterator`
        // will never return `None`, and `Some` is always returned.
        match self.record_batches.next() {
            Some(x) => Option::from(PyArrowType(x)),
            None => Option::from(None),
        }
    }
}

#[pyclass]
struct ItemIterator {
    iter: Box<dyn Iterator<Item = PyArrowType<RecordBatch>> + Send>,
}

#[pymethods]
impl ItemIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        println!("__iter__");
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyArrowType<RecordBatch>> {
        println!("__next__");
        slf.iter.next()
    }
}

#[pyfunction]
fn get_numbers() -> ItemIterator {
    let i = RecordBachIterator::new("file".to_string());
    ItemIterator { iter: Box::new(i) }
}


/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn avro_to_arrow(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(create_arrow_table_py, m)?)?;
    m.add_function(wrap_pyfunction!(get_numbers, m)?)?;
    Ok(())
}
