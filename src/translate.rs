use arrow::datatypes::{DataType, Field, Schema};

use super::stata::{Var, VarType};

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

