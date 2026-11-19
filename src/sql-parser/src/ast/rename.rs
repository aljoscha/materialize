// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Schema-name rewrites in persisted CREATE statements.

use super::visit_mut::{self, VisitMut};
use super::{AstInfo, Expr, Ident, Raw, RawDataType, RawItemName, Statement, UnresolvedItemName};

/// Why a statement cannot be rewritten.
#[derive(Debug)]
pub enum RenameSchemaError {
    NotCreate,
    Ambiguous((String, String)),
}

/// Given a [`Statement`] rewrites all references of the schema name `cur_schema_name` to
/// `new_schema_name`.
pub fn rename_schema_refs(
    create_stmt: &mut Statement<Raw>,
    database: &str,
    cur_schema: &str,
    new_schema: &str,
) -> Result<(), RenameSchemaError> {
    match create_stmt {
        stmt @ Statement::CreateConnection(_)
        | stmt @ Statement::CreateDatabase(_)
        | stmt @ Statement::CreateSchema(_)
        | stmt @ Statement::CreateWebhookSource(_)
        | stmt @ Statement::CreateSource(_)
        | stmt @ Statement::CreateSubsource(_)
        | stmt @ Statement::CreateSink(_)
        | stmt @ Statement::CreateMetricSink(_)
        | stmt @ Statement::CreateView(_)
        | stmt @ Statement::CreateMaterializedView(_)
        | stmt @ Statement::CreateTable(_)
        | stmt @ Statement::CreateTableFromSource(_)
        | stmt @ Statement::CreateIndex(_)
        | stmt @ Statement::CreateType(_)
        | stmt @ Statement::CreateSecret(_) => {
            let mut visitor = CreateSqlRewriteSchema {
                database,
                cur_schema,
                new_schema,
                error: None,
            };
            visitor.visit_statement_mut(stmt);

            if let Some(e) = visitor.error.take() {
                Err(RenameSchemaError::Ambiguous(e))
            } else {
                Ok(())
            }
        }
        _ => Err(RenameSchemaError::NotCreate),
    }
}

struct CreateSqlRewriteSchema<'a> {
    database: &'a str,
    cur_schema: &'a str,
    new_schema: &'a str,
    error: Option<(String, String)>,
}

impl<'a> CreateSqlRewriteSchema<'a> {
    fn maybe_rewrite_idents(&mut self, name: &mut [Ident]) {
        match name {
            [schema, item] if schema.as_str() == self.cur_schema => {
                // TODO(parkmycar): I _think_ when the database component is not specified we can
                // always infer we're using the current database. But I'm not positive, so for now
                // we'll bail in this case.
                if self.error.is_none() {
                    self.error = Some((schema.to_string(), item.to_string()));
                }
            }
            [database, schema, _item] => {
                if database.as_str() == self.database && schema.as_str() == self.cur_schema {
                    *schema = Ident::new_unchecked(self.new_schema);
                }
            }
            _ => (),
        }
    }
}

impl<'a, 'ast> VisitMut<'ast, Raw> for CreateSqlRewriteSchema<'a> {
    fn visit_expr_mut(&mut self, e: &'ast mut Expr<Raw>) {
        match e {
            Expr::Identifier(id) => {
                // The last ID component is a column name that should not be
                // considered in the rewrite.
                let i = id.len() - 1;
                self.maybe_rewrite_idents(&mut id[..i]);
            }
            Expr::QualifiedWildcard(id) => {
                self.maybe_rewrite_idents(id);
            }
            _ => visit_mut::visit_expr_mut(self, e),
        }
    }

    fn visit_unresolved_item_name_mut(
        &mut self,
        unresolved_item_name: &'ast mut UnresolvedItemName,
    ) {
        self.maybe_rewrite_idents(&mut unresolved_item_name.0);
    }

    fn visit_data_type_mut(&mut self, data_type: &'ast mut RawDataType) {
        // The generated `visit_data_type_mut` is a no-op because `DataType` is an
        // associated type the generic visitor treats opaquely, so we must descend
        // by hand. A type is referenced by a schema-qualified name precisely in
        // these data type positions (a cast, a column type, or a nested element
        // type), so without this a schema rename leaves stale references in any
        // dependent that uses one of the schema's types.
        match data_type {
            RawDataType::Array(element_type) | RawDataType::List(element_type) => {
                self.visit_data_type_mut(element_type)
            }
            RawDataType::Map {
                key_type,
                value_type,
            } => {
                self.visit_data_type_mut(key_type);
                self.visit_data_type_mut(value_type);
            }
            RawDataType::Other { name, .. } => self.visit_item_name_mut(name),
        }
    }

    fn visit_item_name_mut(&mut self, item_name: &'ast mut <Raw as AstInfo>::ItemName) {
        match item_name {
            RawItemName::Name(n) | RawItemName::Id(_, n, _) => self.maybe_rewrite_idents(&mut n.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::display::AstDisplay;
    use crate::parser::parse_statements;

    #[mz_ore::test]
    fn rewrite_only_qualified_references_in_target_database() {
        let mut statements = parse_statements(
            "CREATE VIEW db.other.v AS SELECT db.old.t.x, elsewhere.old.t.x, 'db.old.t' FROM db.old.t",
        ).unwrap();
        let stmt = &mut statements[0].ast;
        rename_schema_refs(stmt, "db", "old", "new").unwrap();
        assert_eq!(
            stmt.to_ast_string_stable(),
            r#"CREATE VIEW "db"."other"."v" AS SELECT "db"."new"."t"."x", "elsewhere"."old"."t"."x", 'db.old.t' FROM "db"."new"."t""#
        );
    }

    #[mz_ore::test]
    fn ambiguous_two_part_reference() {
        let mut statements =
            parse_statements("CREATE VIEW db.other.v AS SELECT * FROM old.t").unwrap();
        assert!(matches!(
            rename_schema_refs(&mut statements[0].ast, "db", "old", "new"),
            Err(RenameSchemaError::Ambiguous(_))
        ));
    }
}
