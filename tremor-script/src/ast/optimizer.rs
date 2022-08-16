use crate::ast::visitors::{ArrayAdditionOptimizer, ConstFolder};
use crate::ast::{
    CreationalWith, DefinitionalArgs, Expr, FnDefn, Helper, ImutExpr, Query, Script,
    ScriptDefinition, SelectStmt, Stmt, WindowDefinition,
};
use crate::errors::Result;

/// Runs optimisers on a given AST element
pub struct Optimizer<'run, 'script, 'registry>
where
    'script: 'run,
{
    helper: &'run Helper<'script, 'registry>,
}

// Implemented as a macro, so we don't have to move the optimizers into an array/vec where they'd
// have to be Boxed
macro_rules! walk_thing(
    ($name:ident, $ty:ident, { $additional_uses:stmt }) => {
        walk_thing!($name, $ty, $name, { $additional_uses });
    };
    ($name:ident, $ty:ident, $walker_name:ident, { $additional_uses:stmt }) => {
        /// Walk $name
        /// # Errors
        /// Fails if the optimizer function fails
        pub fn $name(self, walkable: &mut $ty<'script>) -> Result<()> {
            $additional_uses

            ConstFolder::new(self.helper).$walker_name(walkable)?;
            ArrayAdditionOptimizer{}.$walker_name(walkable)?;

            Ok(())
        }
    }
);

impl<'run, 'script, 'registry> Optimizer<'run, 'script, 'registry>
where
    'script: 'run,
{
    #[must_use]
    /// Create a new instance
    pub fn new(helper: &'run Helper<'script, 'registry>) -> Self {
        Self { helper }
    }

    walk_thing!(walk_stmt, Stmt, {
        use crate::ast::walkers::query::Walker as QueryWalker;
    });
    walk_thing!(walk_expr, Expr, {
        use crate::ast::walkers::expr::Walker as ExprWalker;
    });
    walk_thing!(walk_imut_expr, ImutExpr, walk_expr, {
        use crate::ast::walkers::imut_expr::Walker;
    });
    walk_thing!(walk_query, Query, {
        use crate::ast::walkers::query::Walker as QueryWalker;
    });
    walk_thing!(walk_definitional_args, DefinitionalArgs, {
        use crate::ast::walkers::query::Walker as QueryWalker;
    });
    walk_thing!(walk_script_defn, ScriptDefinition, {
        use crate::ast::walkers::query::Walker as QueryWalker;
    });
    walk_thing!(walk_window_defn, WindowDefinition, {
        use crate::ast::walkers::query::Walker as QueryWalker;
    });
    walk_thing!(walk_creational_with, CreationalWith, {
        use crate::ast::walkers::query::Walker as QueryWalker;
    });
    walk_thing!(walk_select_stmt, SelectStmt, {
        use crate::ast::walkers::query::Walker as QueryWalker;
    });
    walk_thing!(walk_fn_defn, FnDefn, {
        use crate::ast::walkers::expr::Walker;
    });
    walk_thing!(walk_script, Script, {
        use crate::ast::walkers::query::Walker;
    });
}
