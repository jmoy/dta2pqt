
use nom::IResult;
use nom::Finish;

#[derive(Debug)]
pub enum Error {
    ParseError(String),
    DecodeError(String)
}

pub trait DownstreamError {
    type MappedResult;
    fn map_res(self,str:&str) -> Self::MappedResult;
}

impl<I,O> DownstreamError for IResult<I,O> {
    type MappedResult = Result<(I,O),Error>;
    fn map_res(self,s:&str) -> Self::MappedResult{
        self.finish().map_err(|_| {Error::ParseError(String::from(s))})
    }
}

