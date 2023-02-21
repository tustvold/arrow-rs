// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This module contains two main structs: [Buffer] and [MutableBuffer]. A buffer represents
//! a contiguous memory region that can be shared via `offsets`.

mod offset;
pub use offset::*;
mod immutable;
pub use immutable::*;
mod mutable;
pub use mutable::*;
mod ops;
mod scalar;
pub use scalar::*;

pub use ops::*;
mod boolean;
pub use boolean::*;
