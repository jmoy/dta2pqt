use std::cmp::min;
use std::fs;
use std::fs::File;
use std::path::Path;

use mmap_rs::MmapOptions;

use parquet::arrow::arrow_writer::ArrowLeafColumn;

use dta2pqt::translate::make_schema;
use dta2pqt::stata::file::{parse_data, parse_metadata, parse_strls};
use dta2pqt::parquet::write_data;
use dta2pqt::concurrency::{seq_rw_marshall,Sender};

pub mod cli;
use crate::cli::Args;
use clap::Parser;

fn main() {
    let args = Args::parse();
        dta2pqt(&args.infile,&args.outfile);
}

fn dta2pqt(in_path: &Path, out_path: &Path) {
    let infile = File::open(in_path).unwrap();
    let infile_len = fs::metadata(in_path).unwrap().len();
    unsafe {
        let inmap = MmapOptions::new(infile_len.try_into().unwrap())
            .unwrap()
            .with_file(&infile, 0)
            .map()
            .unwrap();

        let res = parse_metadata(&inmap);
        if let Err(e) = res {
            panic!("{:?}",e)
        }
        let (metadata,file_map) = res.unwrap();
        let strl_tab = parse_strls(file_map.strls_buf).unwrap();
        //println!("{:?}",metadata);
        let schema = make_schema(&metadata.vars);
        let mut m = 0;
        let mut tasks = Vec::new();
        while m < metadata.nobs {
            let n = min(m+10000,metadata.nobs);
            let md = &metadata;
            let fm = &file_map;
            let st = &strl_tab;
            tasks.push(move |s:Sender<Vec<ArrowLeafColumn>>| {
                move || {
                    let d = parse_data(md,fm,st,m,n).unwrap();
                    s.send(d).unwrap();
                }
            });
            m = n;
        }
        let mut arrays:Vec<Vec<ArrowLeafColumn>> = Vec::new();
        let mut pusher = |d| {arrays.push(d);};
        let par_avail = usize::from(std::thread::available_parallelism().unwrap());
        seq_rw_marshall(&mut tasks.into_iter(),
                        &mut pusher,
                        par_avail);
        write_data(out_path,arrays, &schema);
    }
}
