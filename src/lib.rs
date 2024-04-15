use std::fs::File;

use arrow::{pyarrow::PyArrowType, record_batch::RecordBatch};
use datafusion::datasource::avro_to_arrow::ReaderBuilder;
use pyo3::prelude::*;

struct RecordBachIterator {
    file: String,
    record_batches: Box<dyn Iterator<Item = RecordBatch> + Send>,
}

impl RecordBachIterator {
    pub fn new(file: String, bach_size: usize) -> Self {
        let f = File::open(file.clone()).unwrap();
        let builder = ReaderBuilder::new()
          .read_schema()
          .with_batch_size(bach_size);
        let reader = builder
          .build::<File>(f)
          .unwrap();

        let record_batches = reader.into_iter().map(|result| result.unwrap());
        Self { file: file.clone(), record_batches: Box::new(record_batches) }
    }
}

impl Iterator for RecordBachIterator {
    type Item = PyArrowType<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let _current = self.file.clone();
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
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyArrowType<RecordBatch>> {
        slf.iter.next()
    }
}

#[pyfunction]
fn avro_to_arrow_batches(file_path: String, batch_size: usize) -> ItemIterator {
    let i = RecordBachIterator::new(file_path, batch_size);
    ItemIterator { iter: Box::new(i) }
}


#[pymodule]
fn avro_to_arrow(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(avro_to_arrow_batches, m)?)?;
    Ok(())
}
