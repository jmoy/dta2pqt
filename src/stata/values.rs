use std::{
    mem::transmute,
    num::{NonZeroI8, NonZeroU8},
};

use nom::{
    bytes::complete::take,
    number::complete::{i8, le_f32, le_f64, le_i16, le_i32, le_u16, le_u64},
    IResult,
};

pub type MissingCode = NonZeroU8;
pub type Value<T> = Result<T,Option<MissingCode>>;

pub fn parse_byte(buf: &[u8]) -> (&[u8], Value<i8>) {
    let x: IResult<&[u8], i8> = i8(buf);
    let (buf, i) = x.unwrap();
    let is_missing = i > 100;
    let missing_code = if i > 101 {
        let u = NonZeroU8::try_from(NonZeroI8::try_from(i - 101).unwrap()).unwrap();
        Some(u)
    } else {
        None
    };
    let ans = if is_missing {Err(missing_code)} else { Ok(i) };
    (buf,ans)
}

pub fn parse_int(buf: &[u8]) -> (&[u8], Value<i16>) {
    let x: IResult<&[u8], i16> = le_i16(buf);
    let (buf, i) = x.unwrap();
    let is_missing = i > 32740;
    let missing_code = if i > 32740 {
        let u = NonZeroU8::try_from(NonZeroI8::try_from((i - 32740) as i8).unwrap()).unwrap();
        Some(u)
    } else {
        None
    };
    let ans = if is_missing { Err(missing_code) } else { Ok(i) };
    (buf,ans)
}

pub fn parse_long(buf: &[u8]) -> (&[u8], Value<i32>) {
    let x: IResult<&[u8], i32> = le_i32(buf);
    let (buf, i) = x.unwrap();
    let is_missing = i > 2147483620;
    let missing_code = if i > 2147483620 {
        let u = NonZeroU8::try_from(NonZeroI8::try_from((i - 2147483620) as i8).unwrap()).unwrap();
        Some(u)
    } else {
        None
    };
    let ans = if is_missing {Err(missing_code)} else {Ok(i)};
    (buf,ans)
}

pub fn parse_float(buf: &[u8]) -> (&[u8], Value<f32>) {
    let x: IResult<&[u8], f32> = le_f32(buf);
    let (buf, f) = x.unwrap();

    let i = unsafe { transmute::<f32, u32>(f) };
    let is_missing = i >= 0x7f_00_00_00;
    let mask = 0x3_ff;
    let j = (i >> 10) & 0xff;
    let missing_code = if j >= 1 && j <= 26 && i&mask == 0 {
        Some(NonZeroU8::try_from(j as u8).unwrap())
    } else {
        None
    };
    let ans = if is_missing { Err(missing_code) } else { Ok(f) };
    (buf,ans)
}

pub fn parse_double(buf: &[u8]) -> (&[u8], Value<f64>) {
    let x: IResult<&[u8], f64> = le_f64(buf);
    let (buf, d) = x.unwrap();

    let i = unsafe { transmute::<f64, u64>(d) };
    let is_missing = i >= 0x7f_e0_00_00_00_00_00_00;
    let mask = 0xff_ff_ff_ff_ff;
    let j = (i >> 40) & 0xff;
    let missing_code = if j >= 1 && j <= 26 && i&mask ==0 {
        Some(NonZeroU8::try_from(j as u8).unwrap())
    } else {
        None
    };
    let ans = if is_missing { Err(missing_code) } else { Ok(d) };
    (buf,ans)
}

// Result is a (o,v) tuple
pub fn parse_strlid(buf: &[u8]) -> (&[u8], (u64, u32)) {
    let x: IResult<&[u8], u16> = le_u16(buf);
    let (buf, v) = x.unwrap();
    let y: IResult<&[u8], &[u8]> = take(6usize)(buf);
    let (buf, o6) = y.unwrap();
    let mut arr: [u8; 8] = [0; 8];
    arr[0..6].clone_from_slice(o6);
    let z: IResult<&[u8], u64> = le_u64(&arr);
    let (_, o) = z.unwrap();
    (buf, (o, v as u32))
}

pub fn bytes_to_string(bs: &[u8]) -> String {
    match bs.iter().position(|&x| x == 0) {
        Some(n) => String::from_utf8(bs[..n].to_vec()).unwrap(),
        None => String::from_utf8(bs.to_vec()).unwrap(),
    }
}
