use crate::raw::{make_decoder, ArrayDecoder};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field};
use serde::de::{MapAccess, Visitor};
use serde::Deserializer;

pub struct StructArrayDecoder {
    data_type: DataType,
    decoders: Vec<(String, Box<dyn ArrayDecoder>)>,
    row_count: usize,
}

impl StructArrayDecoder {
    pub fn new(data_type: DataType, batch_size: usize) -> Result<Self, ArrowError> {
        let decoders = struct_fields(&data_type)
            .iter()
            .map(|f| {
                let name = serde_json::ser::to_string(f.name()).unwrap();
                let decoder = make_decoder(f.data_type().clone(), batch_size)?;
                Ok((name, decoder))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            data_type,
            decoders,
            row_count: 0,
        })
    }
}

impl ArrayDecoder for StructArrayDecoder {
    fn visit(&mut self, row: usize, buf: &str) -> Result<usize, ArrowError> {
        if self.row_count != row {
            todo!()
        }

        let trimmed = row.trim_start();
        let mut offset = row.len() - trimmed.len();
        assert!(trimmed.starts_with('{'), "todo");
        offset += 1;

        while !cur.starts_with('}') {
            let delimiter = trimmed[offset..].find(':').expect("todo");
            let field_name = trimmed[offset..offset + delimiter].trim();
            let decoder = self
                .decoders
                .iter_mut()
                .find_map(|(x, d)| (x == field_name).then_some(d))
                .expect("todo");

            let consumed = decoder.visit(self.row_count, &trimmed[offset + delimiter + 1..])?;
            offset += delimiter + 1 + consumed;
        }

        self.row_count += 1;
        Ok(offset + 1)
    }

    fn flush(&mut self) -> ArrayData {
        todo!()
    }
    // fn decode(&mut self, values: &[Option<&RawValue>]) -> Result<ArrayData, ArrowError> {
    //     let fields = struct_fields(&self.data_type);
    //     let mut children: Vec<Vec<Option<&RawValue>>> = (0..fields.len())
    //         .map(|_| vec![None; values.len()])
    //         .collect();
    //
    //     for (row, value) in values.iter().enumerate() {
    //         match value {
    //             Some(v) => {
    //                 let mut reader = serde_json::de::Deserializer::from_str(v.get());
    //
    //                 let visitor = StructVisitor {
    //                     fields,
    //                     row,
    //                     output: &mut children,
    //                 };
    //
    //                 reader.deserialize_map(visitor).map_err(|_| {
    //                     ArrowError::JsonError(format!(
    //                         "Failed to parse \"{}\" as struct",
    //                         v.get()
    //                     ))
    //                 })?;
    //             }
    //             None => {
    //                 return Err(ArrowError::NotYetImplemented(
    //                     "Struct containing nested nulls is not yet supported".to_string(),
    //                 ))
    //             }
    //         }
    //     }
    //
    //     let child_data = self
    //         .decoders
    //         .iter_mut()
    //         .zip(&children)
    //         .map(|(decoder, values)| decoder.decode(values))
    //         .collect::<Result<Vec<_>, _>>()?;
    //
    //     // Sanity check
    //     child_data
    //         .iter()
    //         .for_each(|x| assert_eq!(x.len(), values.len()));
    //
    //     let data = ArrayDataBuilder::new(self.data_type.clone())
    //         .len(values.len())
    //         .child_data(child_data);
    //
    //     // Safety
    //     // Validated lengths above
    //     Ok(unsafe { data.build_unchecked() })
    // }
}
//
// struct StructVisitor<'de, 'a> {
//     fields: &'a [Field],
//     output: &'a mut [Vec<Option<&'de RawValue>>],
//     row: usize,
// }
//
// impl<'de, 'a> Visitor<'de> for StructVisitor<'de, 'a> {
//     type Value = ();
//
//     // Format a message stating what data this Visitor expects to receive.
//     fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
//         formatter.write_str("an object")
//     }
//
//     fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
//     where
//         M: MapAccess<'de>,
//     {
//         while let Some((key, value)) = access.next_entry::<&'de str, &'de RawValue>()? {
//             // Optimize for the common case of a few fields with short names
//             if let Some(field_idx) = self.fields.iter().position(|x| x.name() == key) {
//                 self.output[field_idx][self.row] = Some(value);
//             }
//         }
//         Ok(())
//     }
// }

fn struct_fields(data_type: &DataType) -> &[Field] {
    match &data_type {
        DataType::Struct(f) => f,
        _ => unreachable!(),
    }
}
