use clap::Parser;

///Convert Stata DTA file to parquet
#[derive(Parser)]
pub struct Args {
    ///The input DTA file
    pub infile: std::path::PathBuf,
    ///The output parquet file
    pub outfile: std::path::PathBuf
}