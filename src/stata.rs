use std::sync::Arc;

pub mod values;
pub mod file;
pub mod error;

#[derive(Debug,Clone, Copy)]
pub enum VarType {
    TStrf(u16),
    TStrl,
    TASCII(u8),
    TByte,
    TInt,
    TLong,
    TFloat,
    TDouble,
}

#[derive(Debug)]
pub struct Var {
    pub ty: VarType,
    pub name: String,
    pub format: String,
    pub value_label: String,
    pub var_label: String,
    pub dictionary: Option<Arc<ValueLabelTable>>,
}
#[derive(Debug)]
pub struct ValueLabelTable {
    pub labelname: String,
    pub labels: Vec<String>,
    pub values: Vec<u32>,
}

