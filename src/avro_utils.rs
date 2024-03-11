use std::{collections::HashMap, fs::File, sync::Arc};
use arrow_array::RecordBatch;
use avro_rs::Reader;
use serde_json::Value;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};


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

fn create_arrow_record_batch() -> RecordBatch {
    let avro_path = "tests/static/data.avro";
    let avro_schema = get_avro_schema_json(avro_path);
    let arrow_schema = avro_schema_to_arrow_schema(&avro_schema);
    // let columns = HashMap<String, Vec<Value>>::new();

    // let batch = RecordBatch::try_new(
    //     Arc::new(arrow_schema),
    //     vec![Arc::new(id_array)]
    // ).unwrap();
    // println!("{:?}", arrow_schema);
    // batch
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
}
