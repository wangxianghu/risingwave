// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{BinaryOperator, Expr};

use crate::binder::Binder;
use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall};

impl Binder {
    pub(super) fn bind_binary_op(
        &mut self,
        left: Expr,
        op: BinaryOperator,
        mut right: Expr,
    ) -> Result<ExprImpl> {
        let bound_left = self.bind_expr_inner(left)?;

        let mut func_types = vec![];

        right = match right {
            Expr::SomeOp(expr) => {
                func_types.push(ExprType::Some);
                *expr
            }
            Expr::AllOp(expr) => {
                func_types.push(ExprType::All);
                *expr
            }
            right => right,
        };

        let bound_right = self.bind_expr_inner(right)?;

        func_types.extend(Self::resolve_binary_operator(
            op,
            &bound_left,
            &bound_right,
        )?);

        FunctionCall::new_binary_op_func(func_types, vec![bound_left, bound_right])
    }

    fn resolve_binary_operator(
        op: BinaryOperator,
        bound_left: &ExprImpl,
        bound_right: &ExprImpl,
    ) -> Result<Vec<ExprType>> {
        let mut func_types = vec![];
        let final_type = match op {
            BinaryOperator::Plus => ExprType::Add,
            BinaryOperator::Minus => ExprType::Subtract,
            BinaryOperator::Multiply => ExprType::Multiply,
            BinaryOperator::Divide => ExprType::Divide,
            BinaryOperator::Modulo => ExprType::Modulus,
            BinaryOperator::NotEq => ExprType::NotEqual,
            BinaryOperator::Eq => ExprType::Equal,
            BinaryOperator::Lt => ExprType::LessThan,
            BinaryOperator::LtEq => ExprType::LessThanOrEqual,
            BinaryOperator::Gt => ExprType::GreaterThan,
            BinaryOperator::GtEq => ExprType::GreaterThanOrEqual,
            BinaryOperator::And => ExprType::And,
            BinaryOperator::Or => ExprType::Or,
            BinaryOperator::Like => ExprType::Like,
            BinaryOperator::NotLike => {
                func_types.push(ExprType::Not);
                ExprType::Like
            }
            BinaryOperator::BitwiseOr => ExprType::BitwiseOr,
            BinaryOperator::BitwiseAnd => ExprType::BitwiseAnd,
            BinaryOperator::BitwiseXor => ExprType::Pow,
            BinaryOperator::PGBitwiseXor => ExprType::BitwiseXor,
            BinaryOperator::PGBitwiseShiftLeft => ExprType::BitwiseShiftLeft,
            BinaryOperator::PGBitwiseShiftRight => ExprType::BitwiseShiftRight,
            BinaryOperator::Arrow => ExprType::JsonbAccessInner,
            BinaryOperator::LongArrow => ExprType::JsonbAccessStr,
            BinaryOperator::Concat => {
                match (bound_left.return_type(), bound_right.return_type()) {
                    // array concatenation
                    (DataType::List { .. }, DataType::List { .. }) => ExprType::ArrayCat,
                    (DataType::List { .. }, _) => ExprType::ArrayAppend,
                    (_, DataType::List { .. }) => ExprType::ArrayPrepend,
                    // string concatenation
                    (DataType::Varchar, _) | (_, DataType::Varchar) => ExprType::ConcatOp,
                    // invalid
                    (left_type, right_type) => {
                        return Err(ErrorCode::BindError(format!(
                            "operator does not exist: {} || {}",
                            left_type, right_type
                        ))
                        .into());
                    }
                }
            }
            BinaryOperator::PGRegexMatch => {
                func_types.push(ExprType::IsNotNull);
                ExprType::RegexpMatch
            }
            BinaryOperator::PGRegexNotMatch => {
                func_types.push(ExprType::IsNull);
                ExprType::RegexpMatch
            }
            _ => {
                return Err(
                    ErrorCode::NotImplemented(format!("binary op: {:?}", op), 112.into()).into(),
                )
            }
        };
        func_types.push(final_type);
        Ok(func_types)
    }
}
