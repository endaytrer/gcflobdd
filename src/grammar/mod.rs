use std::rc::Rc;

use lazy_regex::{regex_captures, regex_is_match};

#[cfg(feature = "fx-hash")]
use rustc_hash::FxHashMap as HashMap;

#[cfg(not(feature = "fx-hash"))]
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grammar {
    pub(crate) root: Rc<GrammarNode>,
}

#[derive(Debug)]
pub struct ParseError(pub String);

impl Grammar {
    fn valid_nonterminal(token: &str) -> bool {
        regex_is_match!(r#"^[A_Z][A-Za-z0-9_]*$"#, token)
    }
    fn valid_bdd(token: &str) -> Option<usize> {
        regex_captures!(r#"^BDD\(([1-9][0-9]+|[2-9])\)$"#, token).map(|(_, v)| v.parse().unwrap())
    }
    fn valid_terminal(token: &str) -> bool {
        token == "a"
    }

    fn parse_rhs_token(
        rhs_token: &str,
        symbol_map: &mut HashMap<String, Rc<GrammarNode>>,
        terminal_node: &mut Option<Rc<GrammarNode>>,
    ) -> Result<Rc<GrammarNode>, ParseError> {
        if let Some(num_vars) = Self::valid_bdd(rhs_token) {
            return Ok(symbol_map
                .entry(rhs_token.to_string())
                .or_insert_with(|| Rc::new(GrammarNode::new(GrammarNodeType::Bdd(num_vars))))
                .clone());
        }
        if Self::valid_terminal(rhs_token) {
            return Ok(terminal_node
                .get_or_insert_with(|| Rc::new(GrammarNode::new(GrammarNodeType::Terminal)).clone())
                .clone());
        }
        if symbol_map.contains_key(rhs_token) {
            return Ok(Rc::clone(symbol_map.get(rhs_token).unwrap()));
        }
        if !Self::valid_nonterminal(rhs_token) {
            return Err(ParseError(format!("invalid symbol {}", rhs_token)));
        }
        Err(ParseError(format!(
            "GCFLOBDD grammar does not support recursive rules, meaning symbol {} should be defined",
            rhs_token
        )))
    }

    fn parse_production(
        production_rule: &str,
        symbol_map: &mut HashMap<String, Rc<GrammarNode>>,
        terminal_node: &mut Option<Rc<GrammarNode>>,
    ) -> Result<Rc<GrammarNode>, ParseError> {
        let mut tokens = production_rule.split_whitespace();
        let lhs = tokens.next().ok_or(ParseError(
            "production rule must start with a symbol".to_string(),
        ))?;

        if symbol_map.contains_key(lhs) {
            return Err(ParseError(format!("symbol {} already exists", lhs)));
        }
        if tokens.next().ok_or(ParseError(
            "production rule must have a \"->\" after LHS".to_string(),
        ))? != "->"
        {
            return Err(ParseError(
                "production rule must have a \"->\" after LHS".to_string(),
            ));
        }
        let rhs_nodes = tokens
            .map(|token| Self::parse_rhs_token(token, symbol_map, terminal_node))
            .collect::<Result<_, _>>()?;
        let grammar = Rc::new(GrammarNode::new(GrammarNodeType::Internal(rhs_nodes)));
        symbol_map.insert(lhs.to_string(), grammar.clone());
        Ok(grammar)
    }
    pub fn new(production_rules: &[String]) -> Result<Self, ParseError> {
        let mut symbol_map = HashMap::default();
        let mut terminal_node = None;
        let mut rules = production_rules.iter();
        let first_rule = rules
            .next()
            .ok_or(ParseError("production rules must not be empty".to_string()))?;
        for production_rule in rules.rev() {
            Self::parse_production(
                production_rule.as_str(),
                &mut symbol_map,
                &mut terminal_node,
            )?;
        }
        Ok(Self {
            root: Self::parse_production(first_rule, &mut symbol_map, &mut terminal_node)?,
        })
    }
    /// The grammar of the vectors that this matrix grammar's matrices act on.
    ///
    /// A matrix grammar addresses a row and a column bit per position; the
    /// halved grammar is the same tree with every `S -> a a` leaf collapsed to
    /// a single terminal, so it addresses exactly one of the two halves and
    /// covers `num_vars() / 2` variables.
    ///
    /// Sharing is preserved: a symbol used twice in the source is one shared
    /// node in the result, so a balanced matrix grammar halves to a balanced
    /// vector grammar (and node tables keep sharing accordingly).
    ///
    /// Panics unless every grouping is binary with `S -> a a` at the leaves --
    /// the shape [`mk_matmul`](crate::gcflobdd::GcflobddT::mk_matmul) requires.
    pub fn halved(&self) -> Self {
        let mut terminal = None;
        let mut memo = HashMap::default();
        Self {
            root: Self::halve_node(&self.root, &mut terminal, &mut memo),
        }
    }

    fn halve_node(
        node: &Rc<GrammarNode>,
        terminal: &mut Option<Rc<GrammarNode>>,
        memo: &mut HashMap<usize, Rc<GrammarNode>>,
    ) -> Rc<GrammarNode> {
        let key = Rc::as_ptr(node) as usize;
        if let Some(halved) = memo.get(&key) {
            return halved.clone();
        }
        let GrammarNodeType::Internal(children) = &node.node else {
            panic!("halved() requires binary groupings, found a BDD or terminal grouping")
        };
        let [g1, g2] = &children[..] else {
            panic!(
                "halved() requires binary groupings, found a rule with {} symbols on the right",
                children.len()
            )
        };
        let terminals = matches!(g1.node, GrammarNodeType::Terminal) as usize
            + matches!(g2.node, GrammarNodeType::Terminal) as usize;
        let halved = match terminals {
            // `S -> a a` addresses one row bit and one column bit; halved, it
            // is a single variable.
            2 => terminal
                .get_or_insert_with(|| Rc::new(GrammarNode::new(GrammarNodeType::Terminal)))
                .clone(),
            0 => Rc::new(GrammarNode::new(GrammarNodeType::Internal(vec![
                Self::halve_node(g1, terminal, memo),
                Self::halve_node(g2, terminal, memo),
            ]))),
            _ => panic!(
                "halved() requires a grouping to hold either two terminals or two non-terminals"
            ),
        };
        memo.insert(key, halved.clone());
        halved
    }

    pub fn new_bdd(size: usize) -> Self {
        Self {
            root: Rc::new(GrammarNode {
                num_vars: size,
                node: GrammarNodeType::Bdd(size),
            }),
        }
    }
    /// Total number of boolean variables addressed by this grammar (the flat
    /// variable index space is `0..num_vars`).
    pub fn num_vars(&self) -> usize {
        self.root.num_vars
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_valid_nonterminal() {
        assert!(Grammar::valid_nonterminal("A"));
        assert!(!Grammar::valid_nonterminal("a_123"));
        assert!(!Grammar::valid_nonterminal("1A23"));
    }
    #[test]
    fn test_valid_bdd() {
        assert!(Grammar::valid_bdd("BDD(1)").is_none());
        assert!(Grammar::valid_bdd("BDD(2)") == Some(2));
        assert!(Grammar::valid_bdd("BDD(123)") == Some(123));
        assert!(Grammar::valid_bdd("BDD(123a").is_none());
    }
    #[test]
    fn test_gen_grammar() {
        let grammar = Grammar::new(&["S1 -> S0 S0".to_string(), "S0 -> a".to_string()]).unwrap();
        assert_eq!(grammar.root.num_vars, 2);

        let grammar = Grammar::new(&[
            "S3 -> S2 S2 BDD(10)".to_string(),
            "S2 -> S1 S0".to_string(), // 3
            "S1 -> S0 S0".to_string(), // 2
            "S0 -> a".to_string(),
        ])
        .unwrap();
        assert_eq!(grammar.root.num_vars, 16);
        // should not contain recursive rule
        Grammar::new(&["S1 -> S1".to_string(), "S0 -> a".to_string()]).unwrap_err();
    }

    #[test]
    fn test_halved() {
        // The smallest matrix grammar halves to a bare terminal.
        let grammar = Grammar::new(&["S0 -> a a".to_string()]).unwrap();
        let halved = grammar.halved();
        assert_eq!(halved.num_vars(), 1);
        assert!(matches!(halved.root.node, GrammarNodeType::Terminal));

        // Balanced in, balanced out -- and the two groupings of each rule stay
        // the *same* node, or node tables would lose their sharing.
        let grammar = Grammar::new(&[
            "S2 -> S1 S1".to_string(),
            "S1 -> S0 S0".to_string(),
            "S0 -> a a".to_string(),
        ])
        .unwrap();
        let halved = grammar.halved();
        assert_eq!(halved.num_vars(), grammar.num_vars() / 2);
        let GrammarNodeType::Internal(children) = &halved.root.node else {
            panic!("expected an internal node")
        };
        assert!(Rc::ptr_eq(&children[0], &children[1]));

        // Unbalanced splits are mirrored, halving each grouping.
        let grammar = Grammar::new(&[
            "S -> A B".to_string(),
            "A -> C C".to_string(),
            "C -> a a".to_string(),
            "B -> a a".to_string(),
        ])
        .unwrap();
        let halved = grammar.halved();
        assert_eq!(halved.num_vars(), 3);
        let GrammarNodeType::Internal(children) = &halved.root.node else {
            panic!("expected an internal node")
        };
        assert_eq!(children[0].num_vars, 2);
        assert_eq!(children[1].num_vars, 1);
    }

    #[test]
    #[should_panic(expected = "binary groupings")]
    fn test_halved_rejects_non_binary() {
        Grammar::new(&["S -> a a a".to_string()]).unwrap().halved();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct GrammarNode {
    pub num_vars: usize,
    pub node: GrammarNodeType,
}

pub(crate) type InternalGrammarNodeType = Vec<Rc<GrammarNode>>;
pub(crate) type BddGrammarNodeType = usize;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum GrammarNodeType {
    Internal(InternalGrammarNodeType),
    Bdd(BddGrammarNodeType),
    Terminal,
}

impl GrammarNode {
    fn new(node: GrammarNodeType) -> Self {
        Self {
            num_vars: match &node {
                GrammarNodeType::Bdd(num_vars) => *num_vars,
                GrammarNodeType::Internal(grammar_nodes) => {
                    grammar_nodes.iter().map(|node| node.num_vars).sum()
                }
                GrammarNodeType::Terminal => 1,
            },
            node,
        }
    }
}
