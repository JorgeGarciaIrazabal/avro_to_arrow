use std::{collections::HashMap, fs::File, sync::Arc};


use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use avro_rs::Reader;
use serde_json::Value;

enum AnyTypes {
    Int64(i64),
    Boolean(bool),
    Float64(f64),
}


fn get_avro_schema_json(avro_path: &str) -> Value {
    let f = File::open(avro_path).unwrap();
    let reader = Reader::new(f).unwrap();
    let schema = reader.writer_schema();
    let json = serde_json::to_value(schema).unwrap_or_else(|e| panic!("cannot parse Schema from JSON: {0}", e));
    json
}

fn avro_schema_to_arrow_schema(avro_schema: &Value) -> ArrowSchema {
    let fields = avro_schema["fields"].as_array().unwrap();
    let mut arrow_schema: HashMap<String, Field> = HashMap::new();
    println!("{:?}", avro_schema);
    for field in fields {
        let name = field["name"].as_str().unwrap();
        let type_ = field["type"].clone();
        let mut nullable = false;
        let mut avro_type = "";
        if let Some(type_iter) = type_.as_array() {
            for t in type_iter.iter() {
                if t == "null" {
                    nullable = true;
                } else {
                    avro_type = t.as_str().unwrap();
                }
            }
        } else {
            avro_type = field["type"].as_str().unwrap();
        }
        
        let arrow_type = match avro_type {
            "null" => DataType::Null,
            "int" => DataType::Int32,
            "long" => DataType::Int64,
            "boolean" => DataType::Boolean,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "bytes" => DataType::Binary,
            "string" => DataType::Utf8,
            _ => DataType::Null,
        };
        arrow_schema.insert(name.to_string(), Field::new(name, arrow_type, nullable));
    }
    ArrowSchema::new(arrow_schema.values().cloned().collect::<Vec<Field>>())
    
}


fn avro_to_columnar(rows: usize, avro_path: &str) -> HashMap<String, Vec<AnyTypes>>  {
    let f = File::open(avro_path).unwrap();
    let reader = Reader::new(f).unwrap();
    let mut new_columns: HashMap<String, Vec<AnyTypes>> = HashMap::new();
    // new_columns.insert("column1", Vec::new());
    // new_columns.insert("column3", Vec::new());
    // new_columns.insert("column4", Vec::new());

    for value in reader.take(rows) {
        let v = value.unwrap();
        if let avro_rs::types::Value::Record(record) = v {
            for (key, v) in record.iter() {
                let k = key.to_string(); // Clone the key
                match v {
                    avro_rs::types::Value::Long(i) => {
                        // new_columns[k.as_str()].push(AnyTypes::Int64(*i));
                    },
                    avro_rs::types::Value::Boolean(i) => {
                        // new_columns[k.as_str()].push(AnyTypes::Boolean(*i));
                    },
                    avro_rs::types::Value::Float(i) => {
                        // new_columns[k.as_str()].push(AnyTypes::Float64(*i as f64));
                    },
                    avro_rs::types::Value::Union(u) => {
                        match &**u {
                            avro_rs::types::Value::Null => {
                                // Handle Null case
                            },
                            avro_rs::types::Value::Long(i) => {
                                new_columns.entry(k).or_insert_with(Vec::new).push(AnyTypes::Int64(*i as i64));
                            },
                            avro_rs::types::Value::Boolean(i) => {
                                new_columns.entry(k).or_insert_with(Vec::new).push(AnyTypes::Boolean(*i));
                            },
                            avro_rs::types::Value::Double(i) => {
                                new_columns.entry(k).or_insert_with(Vec::new).push(AnyTypes::Float64(*i as f64));
                            },
                            // Handle other cases
                            _ => panic!("Unsupported data type"),
                        }
                    },
                    // Handle other types as needed
                    _ => panic!("Unsupported data type"),
                }
            }
        }
    }
    new_columns
}

fn create_arrow_record_batch(avro_path: &str) -> RecordBatch {
    let avro_schema = get_avro_schema_json(avro_path);
    let arrow_schema = avro_schema_to_arrow_schema(&avro_schema);
    println!("{:#?}", arrow_schema);

    let records = avro_to_columnar(10, avro_path);
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for field in arrow_schema.fields() {
        let column = records.get(field.name()).unwrap();
        let array: ArrayRef = match &column[0] {
            AnyTypes::Int64(_) => {
                let int_vec: Vec<i64> = column.into_iter().map(|x| match x {
                    AnyTypes::Int64(i) => *i,
                    _ => panic!("Mismatched types in column"),
                }).collect();
                Arc::new(Int64Array::from(int_vec)) as ArrayRef
            },
            AnyTypes::Float64(_) => {
                let float_vec: Vec<f64> = column.into_iter().map(|x| match x {
                    AnyTypes::Float64(f) => *f,
                    _ => panic!("Mismatched types in column"),
                }).collect();
                Arc::new(Float64Array::from(float_vec)) as ArrayRef
            },
            AnyTypes::Boolean(_) => {
                let bool_vec: Vec<bool> = column.into_iter().map(|x| match x {
                    AnyTypes::Boolean(b) => *b,
                    _ => panic!("Mismatched types in column"),
                }).collect();
                Arc::new(BooleanArray::from(bool_vec)) as ArrayRef
            },
            // Handle other types as needed
        };
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(arrow_schema), arrays).unwrap();
    batch
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_avro_schema() {
        let avro_path = "tests/static/data.avro";
        let json_schema = get_avro_schema_json(avro_path);

        let expected_columns = vec!["column1", "column3", "column4"];
        let fields = json_schema["fields"].as_array().unwrap();
        let mut actual_columns = Vec::new();
        for field in fields {
            let name = field["name"].as_str().unwrap();
            actual_columns.push(name);
        }
        assert_eq!(expected_columns, actual_columns);        
    }
    #[test]
    fn test_avro_schema_to_arrow_schema() {
        let avro_schema_json = serde_json::json!({
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "field1", "type": "int"},
                {"name": "field2", "type": "string"},
                {"name": "field3", "type": "boolean"}
            ]
        });

        let expected_arrow_schema = ArrowSchema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, false),
            Field::new("field3", DataType::Boolean, false),
        ]);

        let actual_arrow_schema = avro_schema_to_arrow_schema(&avro_schema_json);

        assert_eq!(expected_arrow_schema, actual_arrow_schema);
    }

    #[test]
    fn test_convert_avro_schema_to_arrow_achema() {
        let avro_path = "tests/static/data.avro";
        let avro_schema = get_avro_schema_json(avro_path);
        println!("{:#?}", avro_schema);
        let arrow_schema = avro_schema_to_arrow_schema(&avro_schema);

        let expected_arrow_schema = ArrowSchema::new(vec![
            Field::new("column1", DataType::Int64, true),
            Field::new("column3", DataType::Float64, true),
            Field::new("column4", DataType::Boolean, true),
        ]);

        assert_eq!(expected_arrow_schema, arrow_schema);
    }

    #[test]
    fn test_create_record_batch_generates_10_rows() {
        let avro_path = "tests/static/data.avro";
        let batch = create_arrow_record_batch(avro_path);
        println!("{:#?}", batch);
        assert_eq!(10, batch.num_rows());
    }
}
