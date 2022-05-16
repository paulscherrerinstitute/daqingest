use proc_macro::{Delimiter, Span, TokenStream, TokenTree};
use quote::quote;
use stats_types::*;
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;
use syn::{parse_macro_input, ExprTuple, Ident, Token};

fn parse_name(gr: proc_macro::Group) -> String {
    for tok in gr.stream() {
        match tok {
            TokenTree::Ident(k) => {
                return k.to_string();
            }
            _ => {}
        }
    }
    panic!();
}

fn parse_counters(gr: proc_macro::Group, a: &mut Vec<Counter>) {
    for tok in gr.stream() {
        match tok {
            TokenTree::Ident(k) => {
                let x = Counter { name: k.to_string() };
                a.push(x);
            }
            _ => {}
        }
    }
}

fn extend_str(mut a: String, x: impl AsRef<str>) -> String {
    a.push_str(x.as_ref());
    a
}

fn stats_struct_agg_impl(st: &StatsStructDef) -> String {
    let name = format!("{}Agg", st.name);
    let name_inp = &st.name;
    let counters_decl: Vec<_> = st
        .counters
        .iter()
        .map(|x| format!("pub {}: AtomicU64", x.to_string()))
        .collect();
    let counters_decl = counters_decl.join(",\n");
    let inits: Vec<_> = st
        .counters
        .iter()
        .map(|x| format!("{}: AtomicU64::new(0)", x.to_string()))
        .collect();
    let inits = inits.join(",\n");
    let mut code = format!(
        "
pub struct {name} {{
    pub ts_create: Instant,
    pub aggcount: AtomicU64,
    {counters_decl},
}}

impl {name} {{
    pub fn new() -> Self {{
        Self {{
            ts_create: Instant::now(),
            aggcount: AtomicU64::new(0),
            {inits},
        }}
    }}"
    );
    let counters_add = st
        .counters
        .iter()
        .map(|x| {
            format!(
                "self.{}.fetch_add(inp.{}.load(Ordering::Acquire), Ordering::AcqRel);\n",
                x.to_string(),
                x.to_string()
            )
        })
        .fold(String::new(), extend_str);
    let s = format!(
        "
    pub fn push(&self, inp: &{name_inp}) {{
        {counters_add}
    }}
"
    );
    code.push_str(&s);
    code.push_str(
        "
}
",
    );
    code
}

fn stats_struct_impl(st: &StatsStructDef) -> String {
    let name = &st.name;
    let inits: Vec<_> = st
        .counters
        .iter()
        .map(|x| format!("{}: AtomicU64::new(0)", x.to_string()))
        .collect();
    let inits = inits.join(",\n");
    let incers: String = st
        .counters
        .iter()
        .map(|x| {
            let nn = x.to_string();
            format!(
                "
    pub fn {nn}_inc(&mut self) {{
        self.{nn}.fetch_add(1, Ordering::AcqRel);
    }}
    pub fn {nn}_add(&mut self, v: u64) {{
        self.{nn}.fetch_add(v, Ordering::AcqRel);
    }}
"
            )
        })
        .fold(String::new(), |a, x| format!("{}{}", a, x));
    format!(
        "
impl {name} {{
    pub fn new() -> Self {{
        Self {{
            ts_create: Instant::now(),
            {inits}
        }}
    }}

    {incers}
}}
    "
    )
}

fn stats_struct_agg_diff_impl(st: &StatsStruct) -> String {
    let name = format!("{}AggDiff", st.name);
    let name_inp = &st.name;
    let decl = st
        .counters
        .iter()
        .map(|x| format!("{}: AtomicU64,\n", x.name))
        .fold(String::new(), extend_str);

    // TODO the diff method must belong to StructAgg.
    let diffs = st
        .counters
        .iter()
        .map(|x| {
            format!(
                "
    pub fn diff_against(&self, k: &Self) -> {name} {{

    }}
"
            )
        })
        .fold(String::new(), extend_str);
    let code = format!(
        "
pub struct {name} {{
    pub dt: AtomicU64,
    pub aggcount: AtomicU64,
    {decl},
}}

impl {name} {{
    {diffs}
}}
"
    );
    code
}

fn stats_struct_decl_impl(st: &StatsStructDef) -> String {
    let name = &st.name;
    let counters_decl = st
        .counters
        .iter()
        .map(|x| format!("pub {}: AtomicU64,\n", x.to_string()))
        .fold(String::new(), extend_str);
    let structt = format!(
        "
pub struct {name} {{
    pub ts_create: Instant,
    {counters_decl}
}}
"
    );
    let code = format!(
        "{}\n\n{}\n\n{}",
        structt,
        stats_struct_impl(st),
        stats_struct_agg_impl(st)
    );
    code
}

fn stats_struct_def(st: &StatsStruct) -> String {
    let name_def = format!("{}", st.name);
    let structt = format!(
        "
const {name_def}: StatsStructDef = StatsStructDef {{
    name: String::new(),
    counters: vec![],
}};
"
    );
    structt
}

fn ident_from_expr(inp: syn::Expr) -> syn::Result<syn::Ident> {
    use syn::spanned::Spanned;
    match inp {
        syn::Expr::Path(k) => {
            if k.path.segments.len() == 1 {
                Ok(k.path.segments[0].ident.clone())
            } else {
                Err(syn::Error::new(k.span(), "Expect identifier"))
            }
        }
        _ => Err(syn::Error::new(inp.span(), "Expect identifier")),
    }
}

fn idents_from_exprs(inp: PunctExpr) -> syn::Result<Vec<syn::Ident>> {
    let mut ret = vec![];
    for k in inp {
        let g = ident_from_expr(k)?;
        ret.push(g);
    }
    Ok(ret)
}

fn func_name_from_expr(inp: syn::Expr) -> syn::Result<syn::Ident> {
    use syn::spanned::Spanned;
    use syn::{Error, Expr};
    match inp {
        Expr::Path(k) => {
            if k.path.segments.len() != 1 {
                return Err(Error::new(k.span(), "Expect function name"));
            }
            let res = k.path.segments[0].ident.clone();
            Ok(res)
        }
        _ => {
            return Err(Error::new(inp.span(), "Expect function name"));
        }
    }
}

fn extract_some_stuff_as_string(inp: impl IntoIterator<Item = syn::Expr>) -> Result<Vec<String>, syn::Error> {
    use syn::spanned::Spanned;
    use syn::{Error, Expr};
    let inp: Vec<_> = inp.into_iter().collect();
    let args: Vec<_> = inp
        .into_iter()
        .map(|k| match k {
            Expr::Path(k) => {
                let sp = k.span();
                if k.path.segments.len() != 1 {
                    return Err(Error::new(sp, "Expect function name with one segment"));
                }
                let res = k.path.segments[0].ident.clone();
                Ok(res)
            }
            _ => {
                return Err(Error::new(k.span(), format!("Expect function name Path {k:?}")));
            }
        })
        .collect();
    for k in &args {
        if k.is_err() {
            return Err(k.clone().unwrap_err());
        }
    }
    let args = args.into_iter().map(Result::unwrap).map(|x| x.to_string()).collect();
    Ok(args)
}

fn func_args_from_expr(inp: Punctuated<syn::Expr, syn::token::Comma>) -> Result<Vec<syn::Ident>, syn::Error> {
    use syn::spanned::Spanned;
    use syn::{Error, Expr};
    let inp: Vec<_> = inp.into_iter().collect();
    let args: Vec<_> = inp
        .into_iter()
        .map(|k| match k {
            Expr::Path(k) => {
                let sp = k.span();
                if k.path.segments.len() != 1 {
                    return Err(Error::new(sp, "Expect function name with one segment"));
                }
                let res = k.path.segments[0].ident.clone();
                Ok(res)
            }
            _ => {
                return Err(Error::new(k.span(), format!("Expect function name Path {k:?}")));
            }
        })
        .collect();
    for k in &args {
        if k.is_err() {
            return Err(k.clone().unwrap_err());
        }
    }
    let args = args.into_iter().map(Result::unwrap).collect();
    Ok(args)
}

type PunctExpr = syn::punctuated::Punctuated<syn::Expr, syn::token::Comma>;

struct FuncCallWithArgs {
    name: Ident,
    args: PunctExpr,
}

impl FuncCallWithArgs {
    fn from_expr(inp: syn::Expr) -> Result<Self, syn::Error> {
        use syn::spanned::Spanned;
        use syn::{Error, Expr};
        let span_all = inp.span();
        match inp {
            Expr::Call(k) => {
                let name = func_name_from_expr(*k.func)?;
                let args = k.args;
                let ret = FuncCallWithArgs { name, args };
                Ok(ret)
            }
            _ => {
                return Err(Error::new(span_all, format!("BAD  {:?}", inp)));
            }
        }
    }
}

#[derive(Debug)]
struct StatsStructDef {
    name: syn::Ident,
    counters: Vec<syn::Ident>,
}

impl StatsStructDef {
    fn from_args(inp: PunctExpr) -> syn::Result<Self> {
        let mut name = None;
        let mut counters = None;
        for k in inp {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "name" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                name = Some(ident);
            }
            if fa.name == "counters" {
                let idents = idents_from_exprs(fa.args)?;
                counters = Some(idents);
            }
        }
        let ret = StatsStructDef {
            name: name.expect("Expect name for StatsStructDef"),
            counters: counters.unwrap_or(vec![]),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
struct StatsTreeDef {
    stats_struct_defs: Vec<StatsStructDef>,
}

impl syn::parse::Parse for StatsTreeDef {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        let k = inp.parse::<syn::ExprTuple>()?;
        let mut a = vec![];
        for k in k.elems {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "StatsStruct" {
                let stats_struct_def = StatsStructDef::from_args(fa.args)?;
                a.push(stats_struct_def);
            } else {
                return Err(syn::Error::new(fa.name.span(), "Unexpected"));
            }
        }
        let ret = StatsTreeDef { stats_struct_defs: a };
        Ok(ret)
    }
}

#[proc_macro]
pub fn stats_struct2(ts: TokenStream) -> TokenStream {
    let def: StatsTreeDef = parse_macro_input!(ts);
    //panic!("DEF: {def:?}");
    let mut ts1 = TokenStream::new();
    for k in def.stats_struct_defs {
        let s = stats_struct_decl_impl(&k);
        let ts2: TokenStream = s.parse().unwrap();
        ts1.extend(ts2);
    }
    let _ts3 = TokenStream::from(quote!(
        mod asd {}
    ));
    ts1
}

#[proc_macro]
pub fn stats_struct(ts: TokenStream) -> TokenStream {
    use std::fmt::Write;
    let mut counters = vec![];
    let mut log = String::new();
    let mut in_name = false;
    let mut in_counters = false;
    let mut name = None;
    for tt in ts.clone() {
        match tt {
            TokenTree::Ident(k) => {
                if k.to_string() == "name" {
                    in_name = true;
                }
                if k.to_string() == "counters" {
                    in_counters = true;
                }
            }
            TokenTree::Group(k) => {
                if in_counters {
                    in_counters = false;
                    parse_counters(k, &mut counters);
                } else if in_name {
                    in_name = false;
                    name = Some(parse_name(k));
                }
            }
            TokenTree::Punct(..) => (),
            TokenTree::Literal(k) => {
                if in_name {
                    write!(log, "NAME write literal {}\n", k);
                    name = Some(k.to_string());
                }
            }
        }
    }
    let name = name.unwrap();
    let stats_struct = StatsStruct { name, counters };
    write!(log, "{:?}\n", stats_struct);
    //panic!("{}", make_code(&stats_struct));
    //panic!("{}", log);
    //let ts2 = TokenStream::from_iter(ts.into_iter());
    //TokenTree::Group(proc_macro::Group::new(Delimiter::Brace, TokenStream::new()));
    stats_struct_def(&stats_struct).parse().unwrap()
}
