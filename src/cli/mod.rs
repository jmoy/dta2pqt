use clap::Parser;
use parquet::basic::{Compression,GzipLevel,ZstdLevel,BrotliLevel};

use nom::bytes::complete as nombc;
use nom::character::complete as nomcc;

const DEFAULT_GZIP_LEVEL:i32 = 6;
const DEFAULT_ZSTD_LEVEL:i32 = 3;
const DEFAULT_BROTLI_LEVEL:i32 = 4;


///Convert Stata DTA file to parquet
#[derive(Parser)]
pub struct Args {
    ///The input DTA file
    pub infile: std::path::PathBuf,
    ///The output parquet file
    pub outfile: std::path::PathBuf,
    ///Default compression
    #[arg(value_parser = compression_parser)]
    pub compression: Option<Compression>
}

fn compression_parser(s: &str) -> Result<Compression, &'static str> {
    match p_compress(s){
        Ok((_,c)) => Ok(c),
        Err(_) => Err("Invalid compression parameter")
    }
}

fn p_compress(s: &str) -> nom::IResult<&str,Compression> {
    let (s,c) = nom::branch::alt((
        p_snappy,p_lzo,p_lz4,p_lz4_raw,
        p_gzip,p_zstd,p_brotli
    ))(s)?;
    let (s,_) = nom::combinator::eof(s)?;
    Ok((s,c))
}
 
fn p_snappy(s: &str) -> nom::IResult<&str,Compression> {
    let (s,_) = nombc::tag_no_case("snappy")(s)?;
    Ok((s,Compression::SNAPPY))
}

fn p_lzo(s: &str) -> nom::IResult<&str,Compression> {
    let (s,_) = nombc::tag_no_case("lzo")(s)?;
    Ok((s,Compression::LZO))
}

fn p_lz4(s: &str) -> nom::IResult<&str,Compression> {
    let (s,_) = nombc::tag_no_case("lz4")(s)?;
    Ok((s,Compression::LZ4))
}

fn p_lz4_raw(s: &str) -> nom::IResult<&str,Compression> {
    let (s,_) = nombc::tag_no_case("lz4_raw")(s)?;
    Ok((s,Compression::LZ4_RAW))
}

fn p_gzip(s: &str) -> nom::IResult<&str,Compression> {
    let (s,_) = nombc::tag_no_case("gzip")(s)?;
    let (s,lvl) = p_optno(s,DEFAULT_GZIP_LEVEL)?;
    let lvl = match GzipLevel::try_new(lvl.try_into().unwrap()) {
        Ok(l) => l,
        Err(_) => panic!("Invalid gzip level")
    };
    Ok((s,Compression::GZIP(lvl)))
}

fn p_zstd(s: &str) -> nom::IResult<&str,Compression> {
    let (s,_) = nombc::tag_no_case("zstd")(s)?;
    let (s,lvl) = p_optno(s,DEFAULT_ZSTD_LEVEL)?;
    let lvl = match ZstdLevel::try_new(lvl) {
        Ok(l) => l,
        Err(_) => panic!("Invalid zstd level")
    };
    Ok((s,Compression::ZSTD(lvl)))
}

fn p_brotli(s: &str) -> nom::IResult<&str,Compression> {
    let (s,_) = nombc::tag_no_case("brotli")(s)?;
    let (s,lvl) = p_optno(s,DEFAULT_BROTLI_LEVEL)?;
    let lvl = match BrotliLevel::try_new(lvl.try_into().unwrap()) {
        Ok(l) => l,
        Err(_) => panic!("Invalid brotli level")
    };
    Ok((s,Compression::BROTLI(lvl)))
}

fn p_optno(s:&str,n:i32) -> nom::IResult<&str,i32> {
    nom::branch::alt((
        nom::sequence::delimited(nombc::tag("("),nomcc::i32,nombc::tag(")")),
        nom::combinator::value(n,nom::combinator::eof)
    ))(s)
}

