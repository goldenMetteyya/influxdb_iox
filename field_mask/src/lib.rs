use generated_types::google::{protobuf::FieldMask, FieldViolation};
use std::collections::BTreeSet;

/// The `process_all_matching_fields!` macro allows to partially process
/// structures as indicated at runtime by a field mask.
///
/// A field mask is a list of field paths. A field path is a "." separated
/// sequence of fields. A single invocation of `match_field_mask!` only cares
/// about the first level of the field path. If you want to match nested fields
/// you need to apply the `process_all_matching_fields!` macro again in the
/// match expression body. You can bind the field mask for that inner level by
/// using the `|(ident)` construct.
///
/// The main use case for `process_all_matching_fields` is to match over proto
/// fields. Optional fields (and in `proto3` all `message` fields) are
/// represented with an `Option<T>`. The `process_all_matching_fields!` allows
/// you to specify a `Some(foo)` pattern in the binding. If the pattern doesn't
/// match, a required field violation is raised.
///
/// ```text
/// field_mask::process_all_matching_fields!((request.config, request.update_mask) {
///     (id, _) = .id => set_id(id)?,
///     (name, _) = .name => set_name(name)?,
///     (thing, _) = .thing => set_thing(thing)?,
///     (opt1, _) = .opt1 => if let Some(opt1) = opt1 { set_opt1(opt1) } else { return Err(FieldViolation::required("opt1")).into()); },
///     // same semantics as the above ^^^
///     (Some(opt2), _) => set_opt2(opt2)?,
///     (Some(stuff), sub_mask) = .stuff => field_mask::process_all_matching_fields!((stuff, sub_mask) {
///             (nested_field, _) = .nested_field => set_nested_field(nested_field),
///             // ...
///         });
///     },
/// })
/// ```
///
/// TODO: figure out how to perform compile time exhaustiveness checks.
#[macro_export]
macro_rules! process_all_matching_fields {
    ( ($ctx:expr, $mask:expr) { $( ( $p:pat , $child_mask:pat ) = . $field:ident => $block:expr ),* $(,)? }) => {{
        use $crate::{AsFieldMask, internal};

        let mut visited = std::collections::BTreeSet::new();
        let mask = $mask.as_field_mask()?;
        let ctx = $ctx;

        #[allow(unreachable_code)]
        let res = (
           $(
               {
                   visited.insert(stringify!($field));
                   if internal::match_field(mask, stringify!($field)) {
                       let $child_mask = internal::child_mask(mask, stringify!($field));
                       let $field = &ctx.$field;
                       #[allow(irrefutable_let_patterns)]
                       if let $p = $field {
                           Some($block)
                       } else {
                           return Err(FieldViolation::required(stringify!($field)).into());
                       }
                   } else {
                       None
                   }
               }
           ),*
        );

        let diff = internal::difference(mask, &visited);
        if !diff.is_empty() {
          return Err(FieldViolation{
              field: "update_mask".to_string(),
              description: format!("Unknown field(s): {}", diff.join(", ")),
          }.into());
        }

        res
    }};
}

/// Shortcut for construction of FieldMasks from literal expressions.
#[macro_export]
macro_rules! field_mask {
    ($($e:expr),*) => { generated_types::google::protobuf::FieldMask{paths: vec![$($e.to_string()),*]} };
}

/// Used internally by the `process_all_matching_fields!` macro; it has to be
/// public since it's used by the macro at the expansion site.
///
/// This trait allows the `process_all_matching_fields` to accept both a
/// FieldMask and an Option<FieldMask>.
pub trait AsFieldMask {
    fn as_field_mask(&self) -> Result<&FieldMask, FieldViolation>;
}

impl AsFieldMask for FieldMask {
    fn as_field_mask(&self) -> Result<&FieldMask, FieldViolation> {
        Ok(self)
    }
}

impl AsFieldMask for Option<FieldMask> {
    fn as_field_mask(&self) -> Result<&FieldMask, FieldViolation> {
        self.as_ref()
            .ok_or_else(|| FieldViolation::required("update_mask"))
    }
}

// This module contains the internal implementation of the
// `process_all_matching_fields!` macro, which must be public because it's used
// by the macro at the expansion site.
pub mod internal {
    use super::*;

    /// Returns true if the field matches the top level component of any of the
    /// paths in the field mask.
    pub fn match_field(mask: &FieldMask, field: &str) -> bool {
        mask.paths.iter().any(|path| match_field_name(path, field))
    }

    /// For a field "a.b.c" returns the first part before the first dot, if any
    /// (namely "a").
    fn head(path: &str) -> &str {
        path.split('.').next().unwrap()
    }

    fn match_field_name(path: &str, field: &str) -> bool {
        path == "*" || head(path) == field
    }

    /// Computes the difference between the requested top level fields in the
    /// mask and the fields that have been actually visited by the
    /// `process_all_matching_fields!`.
    /// The special wildcard '*' path is filtered out from the difference.
    pub fn difference<'a>(mask: &'a FieldMask, visited: &BTreeSet<&'a str>) -> Vec<&'a str> {
        mask.paths
            .iter()
            .map(|path| head(path.as_ref()))
            .filter(|path| *path != "*")
            .collect::<BTreeSet<_>>()
            .difference(visited)
            .cloned()
            .collect::<Vec<_>>()
    }

    /// Creates a new `FieldMask` containing all children paths that share
    /// `head`.
    pub fn child_mask(mask: &FieldMask, head: &str) -> FieldMask {
        FieldMask {
            paths: children_fields(mask, head),
        }
    }

    fn children_fields(mask: &FieldMask, head: &str) -> Vec<String> {
        mask.paths
            .iter()
            .filter_map(|i| i.strip_prefix(&format!("{}.", head)))
            .map(|i| i.to_string())
            .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_match_field_name() {
            assert!(match_field_name("*", "a"));
            assert!(match_field_name("a", "a"));
            assert!(match_field_name("a.b", "a"));
            assert!(!match_field_name("b.a", "a"));
        }

        #[test]
        fn test_match_field() {
            assert!(match_field(&field_mask!["a.b", "b.c"], "a"));
            assert!(match_field(&field_mask!["a.b", "b.c"], "b"));
            assert!(!match_field(&field_mask!["a.b", "b.c"], "c"));
            assert!(!match_field(&field_mask![], ""));
        }

        #[test]
        fn test_head() {
            assert_eq!(head("a.b.c"), "a");
        }

        #[test]
        fn test_children_fields() {
            let mask = field_mask!["a.b.c", "a.b.d", "b.c"];
            assert_eq!(children_fields(&mask, "a"), field_mask!["b.c", "b.d"].paths);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(thiserror::Error, Debug)]
    enum Error {
        #[error("bad something")]
        BadSomething,
        #[error("field validation")]
        FieldViolation(#[from] FieldViolation),
    }

    type Result<T> = std::result::Result<T, Error>;

    #[derive(Clone, Debug)]
    struct Config {
        id: u32,
        something: Option<Foo>,
        other: Option<Foo>,
    }

    #[derive(Clone, Debug)]
    struct Foo {
        bar: u32,
        baz: u32,
    }

    fn process_id(id: u32) -> Result<u32> {
        Ok(1 + id)
    }

    fn process_something(something: &Foo, mask: FieldMask) -> Result<String> {
        Ok(format!("{} inner mask {:?}", something.bar, mask.paths))
    }

    fn process_something_err(_something: &Foo, _mask: &FieldMask) -> Result<()> {
        Err(Error::BadSomething)
    }

    fn test_config() -> Config {
        Config {
            id: 42,
            something: Some(Foo { bar: 1, baz: 2 }),
            other: None,
        }
    }

    #[test]
    fn test_success() {
        fn test() -> Result<()> {
            let cfg = test_config();
            let mask = field_mask!["something.other", "id", "other"];

            process_all_matching_fields!((cfg, mask) {
                (id,_) = .id => process_id(*id)?,
                (Some(something), mask) = .something => process_something(&something, mask)?,
                (Some(other), _) = .other  => {println!("{}", 1 + other.bar); unreachable!(".other"); },
            });

            Ok(())
        }
        match test().unwrap_err() {
            Error::FieldViolation(FieldViolation { field, description }) => {
                assert_eq!(field, "other");
                assert_eq!(description, "Field is required");
            }
            e => panic!("{:?}", e),
        };
    }

    #[test]
    // This test just ensures that the macro can cope with parameters being
    // references.
    fn test_references() -> Result<()> {
        let cfg = test_config();
        let mask = field_mask!["id"];

        let res = process_all_matching_fields!((&cfg, &mask) {
            (id, _) = .id => process_id(*id)?,
        });
        assert_eq!(res, Some(43));
        Ok(())
    }

    #[test]
    fn test_optional_mask_present() -> Result<()> {
        let cfg = test_config();
        let mask = Some(field_mask![]);

        let res = process_all_matching_fields!((cfg,mask) {
            (id, _) = .id => {process_id(*id)?; unreachable!(".id");},
        });

        assert_eq!(res, None);
        Ok(())
    }

    #[test]
    fn test_optional_mask_missing() {
        fn test() -> Result<()> {
            let cfg = test_config();
            let mask = None;

            process_all_matching_fields!((cfg, mask) {
                (id, _) = .id => process_id(*id)?,
            });

            Ok(())
        }
        match test().unwrap_err() {
            Error::FieldViolation(FieldViolation { field, description }) => {
                assert_eq!(field, "update_mask");
                assert_eq!(description, "Field is required");
            }
            e => panic!("{:?}", e),
        }
    }

    // Nested fields can be processed recursively.
    #[test]
    fn test_nested_mask() -> Result<()> {
        let cfg = test_config();
        let mask = field_mask!["something.bar"];

        let mut found = 0;
        let res = process_all_matching_fields!((cfg, mask) {
           (Some(something), mask) = .something => {
              process_all_matching_fields!((something, mask) {
                (bar, _) = .bar => { found = *bar; found },
              })
           },
        });
        assert_eq!(found, 1);
        assert_eq!(res, Some(Some(1)));

        Ok(())
    }

    // Code blocks in arms can fail.
    #[test]
    fn test_error_in_body() {
        fn test() -> Result<()> {
            let cfg = test_config();
            let mask = field_mask!["something.other", "id"];

            process_all_matching_fields!((cfg, mask) {
              (id, _) = .id => process_id(*id)?,
              (Some(something), _) = .something => process_something_err(something, &mask)?,
              (Some(other), _) = .other => {println!("{}", 1 + other.bar); unreachable!("should not execute");},
            });

            Ok(())
        }
        match test().unwrap_err() {
            Error::BadSomething => {}
            e => panic!("{:?}", e),
        }
    }

    // If a user provides a field mask with unknown fields we fail at runtime
    #[test]
    fn test_unknown_field_in_mask() {
        fn test() -> Result<()> {
            let cfg = test_config();
            let mask = field_mask!["something", "id"];

            process_all_matching_fields!((cfg, mask) {
              (id, _) = .id => { process_id(*id)?; },
            });

            Ok(())
        }
        match test().unwrap_err() {
            Error::FieldViolation(FieldViolation { field, description }) => {
                assert_eq!(field, "update_mask");
                assert_eq!(description, "Unknown field(s): something");
            }
            e => panic!("{:?}", e),
        };
    }

    #[test]
    fn test_wildcard() -> Result<()> {
        let cfg = test_config();
        let mask = field_mask!["*"];

        let res = process_all_matching_fields!((cfg, mask) {
            (id, _) = .id => id * 10,
            (Some(something), _) = .something => something.bar * 10,
        });
        assert_eq!(res, (Some(420), Some(10)));

        Ok(())
    }
}
