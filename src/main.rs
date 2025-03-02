use std::pin::Pin;

use datafusion::{
    arrow::array::RecordBatch,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::Stream;

pub struct MySpillableStream {
    _field: Option<()>,
}

impl Default for MySpillableStream {
    fn default() -> Self {
        Self {
            _field: Default::default(),
        }
    }
}

impl Stream for MySpillableStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

impl RecordBatchStream for MySpillableStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        todo!()
    }
}

fn try_downcast<'a>(
    rf: &'a mut SendableRecordBatchStream,
) -> Option<&'a mut Pin<Box<MySpillableStream>>> {
    assert_eq!(size_of::<SendableRecordBatchStream>(), 16);
    assert_eq!(size_of::<&dyn RecordBatchStream>(), 16);

    let default = MySpillableStream::default();

    let ref_to_default: &dyn RecordBatchStream = &default;
    

    // SAFETY: asserts at the begining
    let pointer_to_ref_as_bytes: *const [u8; 16] =
        (&ref_to_default as *const &dyn RecordBatchStream) as *const [u8; 16];

    
    // SAFETY: asserts at the begining
    let rf_pointer: *const [u8; 16] =
        (&*rf as *const Pin<Box<dyn RecordBatchStream + Send + 'static>>) as *const [u8; 16];

    let default_ptr_bytes = unsafe { *pointer_to_ref_as_bytes };
    let rf_bytes = unsafe { *rf_pointer };
    if default_ptr_bytes[8..] != rf_bytes[8..] {
        return None;
    }

    let true_pointer = rf_pointer as *const Pin<Box<MySpillableStream>>;
    let true_pointer = true_pointer as *mut Pin<Box<MySpillableStream>>;
    // SAFETY: we have rf with lifetime 'a so we know that thing under it lives at least 'a long
    unsafe { true_pointer.as_mut() }
}

pub struct MyNonspillableStream {
    _field: usize,
}

impl Stream for MyNonspillableStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

impl RecordBatchStream for MyNonspillableStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        todo!()
    }
}

fn main() {
    let stream = MySpillableStream { _field: Some(()) };

    let mut stream: SendableRecordBatchStream = Box::pin(stream);
    assert!(try_downcast(&mut stream).is_some());

    let stream = MyNonspillableStream { _field: 0 };

    let mut stream: SendableRecordBatchStream = Box::pin(stream);
    assert!(try_downcast(&mut stream).is_none());
}
