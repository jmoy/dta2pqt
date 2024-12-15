use arrow::datatypes::{DataType, Field, Schema};

use super::stata::{Var, VarType};

pub struct TranslateSpec {
    pub name: String,
    pub in_type: VarType,
    pub include: bool,
    pub compression: parquet::basic::Compression,
}

impl TranslateSpec {
    pub fn new(v: &Var) -> TranslateSpec {
        TranslateSpec{
            name: v.name.clone(),
            in_type: v.ty,
            include: false,
            compression: parquet::basic::Compression::UNCOMPRESSED,
        }
    }
}  

pub fn make_schema(vars: &[Var]) -> Schema {
    let mut fields: Vec<Field> = Vec::new();
    for v in vars {
        let aty = match v.ty {
            VarType::TStrf(_) => DataType::Utf8,
            VarType::TASCII(_) => DataType::Utf8,
            VarType::TStrl => DataType::Binary,
            VarType::TByte => DataType::Int8,
            VarType::TInt => DataType::Int16,
            VarType::TLong => DataType::Int32,
            VarType::TFloat => DataType::Float32,
            VarType::TDouble => DataType::Float64,
        };
        fields.push(Field::new(&v.name, aty, true));
    }
    return Schema::new(fields);
}

