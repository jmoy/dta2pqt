use std::{fs::OpenOptions, sync::Arc};
use std::path::Path;

use arrow::datatypes::Schema;
use parquet::{arrow::{arrow_to_parquet_schema, arrow_writer::{get_column_writers, ArrowColumnChunk, ArrowLeafColumn}}, basic::Compression, file::{properties::WriterProperties, writer::SerializedFileWriter}};
use rayon::prelude::*;

pub fn write_data(out_path: &Path,
                    to_write:Vec<Vec<ArrowLeafColumn>>,
                    schema: &Schema){
    let parquet_schema = arrow_to_parquet_schema(&schema).unwrap();
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );

    let of = OpenOptions::new()
        .write(true)
        .create(true)
        .open(out_path)
        .unwrap();
    let root_schema = parquet_schema.root_schema_ptr();
    let mut writer = SerializedFileWriter::new(of, root_schema, props.clone()).unwrap();

    // Start row group
    let mut row_group = writer.next_row_group().unwrap();
    let mut col_writers =
        get_column_writers(&parquet_schema, &props, &Arc::new(schema.clone())).unwrap();
   
    col_writers
        .par_iter_mut()
        .enumerate()
        .for_each(|(i,writer)| {
            for arrays in &to_write {
                writer.write(&arrays[i]).unwrap();
            }
        });
    let chunks:Vec<ArrowColumnChunk> = 
                col_writers.into_par_iter()
                    .map(|writer|{
                        return writer.close().unwrap();
                    })
                    .collect();
    chunks.into_iter().for_each(|chunk| {
              chunk.append_to_row_group(&mut row_group).unwrap();
        });
    

    row_group.close().unwrap();

    writer.close().unwrap();
}
