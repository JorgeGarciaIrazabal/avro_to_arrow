use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use std::sync::Arc;
mod avro_utils;

fn create_arrow_table() -> RecordBatch {
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false)
    ]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
    batch
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_arrow_table() {
        let table = create_arrow_table();

        assert_eq!(table.num_columns(), 1);
        assert_eq!(table.num_rows(), 5);
        assert_eq!(table.schema().field(0).name(), "id");
        assert_eq!(table.schema().field(0).data_type(), &DataType::Int32);

        // Assert that the column values are correct
        let id_array = table.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(id_array.values(), &[1, 2, 3, 4, 5]);
    }
}
