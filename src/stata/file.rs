use std::{iter::zip, sync::Arc};

use arrow_array::builder::{
    make_builder, ArrayBuilder, BinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int8Builder, StringBuilder
};
use nom::{
    bytes::complete::{tag, take},
    multi::{many0, many_m_n},
    number::{
        complete::{le_u16, le_u32, le_u8, u8},
        streaming::le_u64,
    },
    IResult,
};
use parquet::arrow::arrow_writer::{compute_leaves, ArrowLeafColumn};

use super::values::{
    bytes_to_string, parse_byte, parse_double, parse_float, parse_int, parse_long, parse_strlid,
};
use super::{
    error::{DownstreamError, Error},
    ValueLabelTable,
};
use super::{Var, VarType};

use super::super::translate::make_schema;

#[derive(Debug)]
pub enum ByteOrder {
    LSF,
    MSF
}
#[derive(Debug)]
pub struct Metadata {
    pub version: u8,
    pub byteorder: ByteOrder,
    pub nvars: usize,
    pub nobs: usize,
    pub vars: Vec<Var>,
    pub rowsize: usize,
    pub datasize: usize,
}

pub struct FileMap<'a> {
    pub data_buf: &'a [u8],
    pub value_labels_buf: &'a [u8],
    pub strls_buf: &'a [u8]
}

#[derive(Debug)]
pub struct StrlEntry<'a> {
    pub v: u32,
    pub o: u64,
    pub is_string: bool,
    pub s: &'a [u8]
}

pub fn parse_metadata(input: &[u8]) -> Result<(Metadata, FileMap), Error> {
    let (_, version) = u8(input).map_res("version")?;
    if version == 113 || version == 114 {
        parse_metadata_old(&input)
    } else if input[0] == b'<' {
        parse_metadata_new(&input)
    } else {
        panic!("Unsupported version")
    }
}

pub fn parse_metadata_new(input: &[u8]) -> Result<(Metadata, FileMap), Error> {
    let start = input;
    let input = parse_tag(input, b"<stata_dta>")?;
    let input = parse_tag(input, b"<header>")?;
    let input = parse_tag(input, b"<release>")?;
    let (input, release) = take(3usize)(input).map_res("release)")?;
    let version = if release == b"118" {
        118
    } else if release == b"117" {
        117
    } else {
        panic!("Unsupported version")
    };
    let input = parse_tag(input, b"</release>")?;

    let input = parse_tag(input, b"<byteorder>")?;
    let (input, byteorder) = take(3usize)(input).map_res("byteorder)")?;
    if byteorder != b"LSF" {
        panic!("Unsupported byteorder")
    }
    let input = parse_tag(input, b"</byteorder>")?;
    let byteorder = ByteOrder::LSF;

    let input = parse_tag(input, b"<K>")?;
    let (input, nvars) = le_u16(input).map_res("K")?;
    let nvars = nvars as usize;
    let input = parse_tag(input, b"</K>")?;

    let input = parse_tag(input, b"<N>")?;
    let (input, nobs) = if version == 117 {
        let (i, n) = le_u32(input).map_res("N")?;
        (i, n as usize)
    } else if version == 118 {
        let (i, n) = le_u64(input).map_res("N")?;
        (i, n as usize)
    } else {
        panic!("Unsupported version")
    };
    let input = parse_tag(input, b"</N>")?;

    let input = parse_tag(input, b"<label>")?;
    let (input, ll) = le_u16(input).map_res("dataset label length")?;
    let (input, dataset_label) = take(ll as usize)(input).map_res("dataset label")?;
    let _dataset_label = bytes_to_string(dataset_label);
    let input = parse_tag(input, b"</label>")?;

    let input = parse_tag(input, b"<timestamp>")?;
    let (input, ll) = u8(input).map_res("dataset time stamp")?;
    let (input, dataset_timestamp) = take(ll as usize)(input).map_res("dataset timestamp")?;
    let _dataset_timestamp = bytes_to_string(dataset_timestamp);
    let input = parse_tag(input, b"</timestamp>")?;
    
    let input = parse_tag(input,b"</header>")?;

    let input = parse_tag(input, b"<map>")?;
    let (input, file_offsets) = many_m_n(14usize, 14usize, le_u64)(input).map_res("map")?;
    let input = parse_tag(input, b"</map>")?;

    let input = parse_tag(input, b"<variable_types>")?;
    let (input, tycodes) = many_m_n(nvars, nvars, le_u16)(input).map_res("variable_types")?;
    let input = parse_tag(input, b"</variable_types>")?;

    let input = parse_tag(input, b"<varnames>")?;
    let (input, names) = many_m_n(nvars, nvars, take(129usize))(input).map_res("varnames")?;
    let input = parse_tag(input, b"</varnames>")?;

    let input = parse_tag(input, b"<sortlist>")?;
    let (input, _srtlist) = many_m_n(nvars + 1, nvars + 1, le_u16)(input).map_res("sortlist")?;
    let input = parse_tag(input, b"</sortlist>")?;

    let input = parse_tag(input, b"<formats>")?;
    let (input, fmtlist) = many_m_n(nvars, nvars, take(57usize))(input).map_res("formats")?;
    let input = parse_tag(input, b"</formats>")?;

    let input = parse_tag(input, b"<value_label_names>")?;
    let (input, flbllist) =
        many_m_n(nvars, nvars, take(129usize))(input).map_res("value_label_names")?;
    let input = parse_tag(input, b"</value_label_names>")?;

    let input = parse_tag(input, b"<variable_labels>")?;
    let (input, lbllist) =
        many_m_n(nvars, nvars, take(321usize))(input).map_res("value_label_names")?;
    let _ = parse_tag(input, b"</variable_labels>")?;

    let mut vars: Vec<Var> = Vec::with_capacity(nvars);
    for i in 0..nvars {
        let tcode = tycodes[i];
        let ty = if tcode >= 1 && tcode <= 2045 {
            VarType::TStrf(tcode)
        } else if tcode == 32768 {
            VarType::TStrl
        } else if tcode == 65530 {
            VarType::TByte
        } else if tcode == 65529 {
            VarType::TInt
        } else if tcode == 65528 {
            VarType::TLong
        } else if tcode == 65527 {
            VarType::TFloat
        } else if tcode == 65526 {
            VarType::TDouble
        } else {
            panic!("Unknown tcode {},{}", i, tcode)
        };
        vars.push(Var {
            ty: ty,
            name: bytes_to_string(names[i]),
            format: bytes_to_string(fmtlist[i]),
            value_label: bytes_to_string(flbllist[i]),
            var_label: bytes_to_string(lbllist[i]),
            dictionary: None,
        });
    }
    let rowsize = calculate_rowsize(&vars);
    let datasize = rowsize * nobs;
    let data_start = (file_offsets[9]+6) as usize; //Skip <data>
    let data_end = (file_offsets[10]-7) as usize; //Skip </data>

    let labels_start = (file_offsets[11]+14) as usize; //Skip <value_labels>
    let labels_end = (file_offsets[12]-15) as usize; //Skip </value_labels>

    let strls_start = (file_offsets[10]+7) as usize;
    let strls_end = (file_offsets[11]-8) as usize;

    Ok((
        Metadata {
            version,
            byteorder,
            nvars,
            nobs,
            vars,
            rowsize,
            datasize,
        },
        FileMap {
            data_buf: &start[data_start..data_end],
            value_labels_buf: &start[labels_start..labels_end],
            strls_buf: &start[strls_start..strls_end]
        },))
}

fn parse_tag<'a>(input: &'a [u8], tag_dat: &[u8]) -> Result<&'a [u8], Error> {
    let (input, _) = tag(tag_dat)(input).map_res(&format!("{:?}", String::from_utf8(tag_dat.to_vec())))?;
    Ok(input)
}

pub fn parse_strls(input: &[u8]) -> Result<Vec<StrlEntry>,Error>{
    let (_,tab) = many0(parse_strl)(input).map_res("strl table")?;
    Ok(tab)
}
fn parse_strl(input: &[u8]) -> IResult<&[u8],StrlEntry> {
    let (input,_) = tag(b"GSO")(input)?;
    let (input,v) = le_u32(input)?;
    let (input,o) = le_u64(input)?;
    let (input,t) = le_u8(input)?;
    let (input,len) = le_u32(input)?;
    let (input,s) = take(len as usize)(input)?;
    Ok((input,StrlEntry{v,o,is_string: t==130,s}))

}
pub fn parse_metadata_old(input: &[u8]) -> Result<(Metadata, FileMap), Error> {
    let (input, version) = u8(input).map_res("version")?;
    let (input, byteorder) = u8(input).map_res("byteorder")?;
    if byteorder != 0x02 {
        panic!("Unsupported byteorder")
    }
    let byteorder = ByteOrder::LSF;
    let (input, _) = tag([0x01])(input).map_res("pad")?;
    let (input, _) = take(1usize)(input).map_res("pad")?;
    let (input, nvars) = le_u16(input).map_res("nvars")?;
    let nvars = nvars as usize;
    let (input, nobs) = le_u32(input).map_res("nobs")?;
    let nobs = nobs as usize;
    //Ignore data label and timestamp
    let (input, _) = take(99usize)(input).map_res("pad")?;

    let (input, tycodes) = many_m_n(nvars, nvars, u8)(input).map_res("typecodes")?;
    let (input, names) = many_m_n(nvars, nvars, take(33usize))(input).map_res("varnames")?;
    let (input, _) = many_m_n(nvars + 1, nvars + 1, le_u16)(input).map_res("sortlist")?;
    let (input, fmtlist) = many_m_n(
        nvars,
        nvars,
        take(if version == 113 { 12usize } else { 49usize }),
    )(input)
    .map_res("formats")?;
    let (input, flbllist) =
        many_m_n(nvars, nvars, take(33usize))(input).map_res("format labels")?;
    let (input, lbllist) = many_m_n(nvars, nvars, take(81usize))(input).map_res("value labels")?;
    let mut vars: Vec<Var> = Vec::with_capacity(nvars);
    for i in 0..nvars {
        let tcode = tycodes[i];
        let ty = if tcode >= 1 && tcode <= 244 {
            VarType::TASCII(tcode)
        } else if tcode == 251 {
            VarType::TByte
        } else if tcode == 252 {
            VarType::TInt
        } else if tcode == 253 {
            VarType::TLong
        } else if tcode == 254 {
            VarType::TFloat
        } else if tcode == 255 {
            VarType::TDouble
        } else {
            panic!("Unknown tcode {},{}", i, tcode)
        };
        vars.push(Var {
            ty: ty,
            name: bytes_to_string(names[i]),
            format: bytes_to_string(fmtlist[i]),
            value_label: bytes_to_string(flbllist[i]),
            var_label: bytes_to_string(lbllist[i]),
            dictionary: None,
        });
    }

    //Skip expansion fields
    let mut input = input;
    let mut dtype;
    let mut len;
    loop {
        (input, dtype) = u8(input).map_res("xfield type")?;
        (input, len) = le_u32(input).map_res("xfield len")?;
        if dtype == 0 {
            break;
        }
        (input, _) = take(len as usize)(input).map_res("xfield")?;
    }
    let rowsize = calculate_rowsize(&vars);
    let datasize = rowsize * nobs;
    Ok((
        Metadata {
            version,
            byteorder,
            nvars,
            nobs,
            vars,
            rowsize,
            datasize,
        },
        FileMap {
            data_buf: &input[..datasize],
            value_labels_buf: &input[datasize..],
            strls_buf: &input[0..0]
        },
    ))
}

pub fn parse_data(
    meta: &Metadata,
    file_map: &FileMap,
    strl_tab: &Vec<StrlEntry>,
    start_row: usize,
    end_row: usize,
) -> Result<Vec<ArrowLeafColumn>, Error> {
    let mut buf = &file_map.data_buf[(start_row * meta.rowsize)..(end_row * meta.rowsize)];
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    let schema = make_schema(&meta.vars);


    for f in schema.fields() {
        builders.push(make_builder(f.data_type(), meta.nobs));
    }
    for _ in 0..(end_row - start_row) {
        //        if (i>10) {
        //            break;
        //        }
        //        println!("Obs {}",i);
        for (f, b) in zip(&meta.vars, &mut builders) {
            match f.ty {
                VarType::TByte => {
                    let d;
                    (buf, d) = parse_byte(buf);
                    //                    print!("{}, ",d);
                    let b = 
                        b.as_any_mut()
                        .downcast_mut::<Int8Builder>()
                        .unwrap();
                    match d {
                        Ok (v) => {b.append_value(v);}
                        Err (_) => {b.append_null();}
                    };
                }
                VarType::TInt => {
                    let d;
                    (buf, d) = parse_int(buf);
                    //                    print!("{}, ",d);
                    let b = 
                        b.as_any_mut()
                        .downcast_mut::<Int16Builder>()
                        .unwrap();
                    match d {
                        Ok (v) => {b.append_value(v);}
                        Err (_) => {b.append_null();}
                    };
                }
                VarType::TLong => {
                    let d;
                    (buf, d) = parse_long(buf);
                    //                    print!("{}, ",d);
                    let b = 
                        b.as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .unwrap();
                    match d {
                        Ok (v) => {b.append_value(v);}
                        Err (_) => {b.append_null();}
                    };
                }
                VarType::TFloat => {
                    let d;
                    (buf, d) = parse_float(buf);
                    //                    print!("{}, ",d);
                    let b = 
                        b.as_any_mut()
                        .downcast_mut::<Float32Builder>()
                        .unwrap();
                    match d {
                        Ok (v) => {b.append_value(v);}
                        Err (_) => {b.append_null();}
                    };
                    
                }
                VarType::TDouble => {
                    let d;
                    (buf, d) = parse_double(buf);
                    //                    print!("{}, ",d);
                    let b = 
                        b.as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap();
                    match d {
                        Ok (v) => {b.append_value(v);}
                        Err (_) => {b.append_null();}
                    };
                }
                VarType::TASCII(n) => {
                    let d: String;
                    let n = n as usize;
                    d = bytes_to_string(&buf[..n]);
                    //                    print!("{}, ",d);
                    buf = &buf[n..];
                    b.as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap()
                        .append_value(d);
                }
                VarType::TStrf(n) => {
                    let d: String;
                    let n = n as usize;
                    d = bytes_to_string(&buf[..n]);
                    //                    print!("{}, ",d);
                    buf = &buf[n..];
                    b.as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap()
                        .append_value(d);
                }
                VarType::TStrl => {
                    let ov: (u64,u32);
                    (buf, ov) = parse_strlid(buf);
                    //                    print!("{}, ",d);
                    let s = if ov==(0,0) {
                        b""
                    } else {
                        let strl_idx = strl_tab
                                        .binary_search_by_key(&ov,|s| {(s.o,s.v)})
                                        .expect(&format!("{ov:?} vo entry not found"));
                        strl_tab[strl_idx].s
                    };
                    b.as_any_mut()
                        .downcast_mut::<BinaryBuilder>()
                        .unwrap()
                        .append_value(s);
                }
            }
        }
        //        print!("\n\n\n");
    }
    let to_write = builders
        .into_iter()
        .zip(&schema.fields)
        .map(|(mut builder, field)| {
            let arr = builder.as_mut().finish();
            return compute_leaves(field, &arr).unwrap();
        })
        .flatten()
        .collect();
    Ok(to_write)
}


pub fn parse_value_labels_oldstyle(input: &[u8]) -> IResult<&[u8], Vec<Arc<ValueLabelTable>>> {
    let (input, tables) = many0(parse_one_value_label_table_oldstyle)(input)?;
    return Ok((input, tables));
}

pub fn parse_one_value_label_table_oldstyle(input: &[u8]) -> IResult<&[u8], Arc<ValueLabelTable>> {
    let (input, _) = take(4usize)(input)?;
    let (input, labname) = take(33usize)(input)?;
    let (input, _) = take(3usize)(input)?;
    let (input, n) = le_u32(input)?;
    let n = n as usize;
    let (input, txtlen) = le_u32(input)?;
    let txtlen = txtlen as usize;
    let (input, offsets) = many_m_n(n, n, le_u32)(input)?;
    let (input, values) = many_m_n(n, n, le_u32)(input)?;
    let (input, txt) = take(txtlen)(input)?;
    let mut labels: Vec<String> = Vec::new();
    for i in 0..n {
        let o = offsets[i] as usize;
        let s = bytes_to_string(&txt[o..]);
        labels.push(s);
    }
    return Ok((
        input,
        Arc::new(ValueLabelTable {
            labelname: bytes_to_string(labname),
            labels,
            values,
        }),
    ));
}

fn calculate_rowsize(vars: &[Var]) -> usize {
    vars.iter()
        .map(|v| match v.ty {
            VarType::TASCII(n) => n as usize,
            VarType::TStrf(n) => n as usize,
            VarType::TStrl => 8usize,
            VarType::TByte => 1usize,
            VarType::TInt => 2usize,
            VarType::TLong => 4usize,
            VarType::TFloat => 4usize,
            VarType::TDouble => 8usize,
        })
        .sum()
}
