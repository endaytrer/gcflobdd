use std::{collections::HashMap, rc::Rc};

use lazy_regex::{regex_captures, regex_is_match};

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
        regex_captures!(r#"^BDD\(([0-9]+)\)$"#, token).map(|(_, v)| v.parse().unwrap())
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
        let mut symbol_map = HashMap::new();
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrammarNode {
    pub num_vars: usize,
    pub node: GrammarNodeType,
}

pub(crate) type InternalGrammarNodeType = Vec<Rc<GrammarNode>>;
pub(crate) type BddNodeType = usize;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GrammarNodeType {
    Internal(InternalGrammarNodeType),
    Bdd(BddNodeType),
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
