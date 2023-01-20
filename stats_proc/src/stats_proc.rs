use proc_macro::TokenStream;
use quote::quote;
use syn::parse::ParseStream;
use syn::{parse_macro_input, Ident};

type PunctExpr = syn::punctuated::Punctuated<syn::Expr, syn::token::Comma>;

struct FuncCallWithArgs {
    name: Ident,
    args: PunctExpr,
}

#[derive(Clone, Debug)]
struct StatsStructDef {
    name: syn::Ident,
    prefix: Option<syn::Ident>,
    counters: Vec<syn::Ident>,
    values: Vec<syn::Ident>,
}

#[derive(Debug)]
struct AggStructDef {
    name: syn::Ident,
    parent: syn::Ident,
    // TODO this currently describes our input (especially the input's name):
    stats: StatsStructDef,
}

#[derive(Debug)]
struct DiffStructDef {
    name: syn::Ident,
    input: syn::Ident,
}

fn extend_str(mut a: String, x: impl AsRef<str>) -> String {
    a.push_str(x.as_ref());
    a
}

fn stats_struct_impl(st: &StatsStructDef) -> String {
    use std::fmt::Write;
    let name = &st.name;
    let inits1 = st
        .counters
        .iter()
        .map(|x| format!("{:12}{}: AtomicU64::new(0)", "", x.to_string()));
    let inits2 = st
        .values
        .iter()
        .map(|x| format!("{:12}{}: AtomicU64::new(0)", "", x.to_string()));
    let inits: Vec<_> = inits1.into_iter().chain(inits2).collect();
    let inits = inits.join(",\n");
    let incers: String = st
        .counters
        .iter()
        .map(|nn| {
            format!(
                "
    pub fn {nn}_inc(&self) {{
        self.{nn}.fetch_add(1, Ordering::AcqRel);
    }}
    pub fn {nn}_add(&self, v: u64) {{
        self.{nn}.fetch_add(v, Ordering::AcqRel);
    }}
    pub fn {nn}_dur(&self, v: Duration) {{
        self.{nn}.fetch_add((v * 1000000).as_secs(), Ordering::AcqRel);
    }}
"
            )
        })
        .fold(String::new(), |mut a, x| {
            a.push_str(&x);
            a
        });
    let values = {
        let mut buf = String::new();
        for nn in &st.values {
            write!(
                buf,
                "
    pub fn {nn}_set(&self, v: u64) {{
        self.{nn}.store(v, Ordering::Release);
    }}
"
            )
            .unwrap();
        }
        buf
    };
    let fn_prometheus = {
        let mut buf = String::new();
        for x in &st.counters {
            let n = x.to_string();
            let pre = if let Some(x) = &st.prefix {
                format!("_{}", x)
            } else {
                String::new()
            };
            buf.push_str(&format!(
                "ret.push_str(&format!(\"daqingest{}_{} {{}}\\n\", self.{}.load(Ordering::Acquire)));\n",
                pre, n, n
            ));
        }
        for x in &st.values {
            let n = x.to_string();
            let nn = if let Some(pre) = &st.prefix {
                format!("{pre}_{n}")
            } else {
                n.to_string()
            };
            buf.push_str(&format!(
                "ret.push_str(&format!(\"daqingest_{} {{}}\\n\", self.{}.load(Ordering::Acquire)));\n",
                nn, n
            ));
        }
        format!(
            "
            pub fn prometheus(&self) -> String {{
                let mut ret = String::new();
                {buf}
                ret
            }}
        "
        )
    };
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

    {values}

    {fn_prometheus}
}}
    "
    )
}

fn stats_struct_decl_impl(st: &StatsStructDef) -> String {
    let name = &st.name;
    let counters_decl = st
        .counters
        .iter()
        .map(|x| format!("{:4}pub {}: AtomicU64,\n", "", x.to_string()))
        .fold(String::new(), extend_str);
    let values_decl = st
        .values
        .iter()
        .map(|x| format!("{:4}pub {}: AtomicU64,\n", "", x.to_string()))
        .fold(String::new(), extend_str);
    let structt = format!(
        "
pub struct {name} {{
    pub ts_create: Instant,
{counters_decl}
{values_decl}
}}

"
    );
    let code = format!("{}\n\n{}", structt, stats_struct_impl(st),);
    code
}

fn agg_decl_impl(st: &StatsStructDef, ag: &AggStructDef) -> String {
    let name = &ag.name;
    let name_inp = &st.name;
    let counters_decl = st
        .counters
        .iter()
        .map(|x| format!("{:4}pub {}: AtomicU64,\n", "", x.to_string()))
        .fold(String::new(), extend_str);
    let mut code = String::new();
    let s = format!(
        "
// Agg decl
pub struct {name} {{
    pub ts_create: Instant,
    pub aggcount: AtomicU64,
{counters_decl}
}}
"
    );
    code.push_str(&s);
    let clone_counters = st
        .counters
        .iter()
        .map(|x| {
            let n = x.to_string();
            format!("{:12}{}: AtomicU64::new(self.{}.load(Ordering::Acquire)),\n", "", n, n)
        })
        .fold(String::new(), extend_str);
    let s = format!(
        "
impl Clone for {name} {{
    fn clone(&self) -> Self {{
        Self {{
            ts_create: self.ts_create.clone(),
            aggcount: AtomicU64::new(self.aggcount.load(Ordering::Acquire)),
{clone_counters}
        }}
    }}
}}
"
    );
    code.push_str(&s);
    let inits = st
        .counters
        .iter()
        .map(|x| format!("{:12}{}: AtomicU64::new(0),\n", "", x.to_string()))
        .fold(String::new(), extend_str);
    let s = format!(
        "
// Agg impl
impl {name} {{
    pub fn new() -> Self {{
        Self {{
            ts_create: Instant::now(),
            aggcount: AtomicU64::new(0),
{inits}
        }}
    }}
"
    );
    code.push_str(&s);
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
        self.aggcount.fetch_add(1, Ordering::AcqRel);
        {counters_add}
    }}
"
    );
    code.push_str(&s);
    {
        let mut buf = String::new();
        for x in &st.counters {
            let n = x.to_string();
            buf.push_str(&format!(
                "ret.push_str(&format!(\"daqingest_{} {{}}\\n\", self.{}.load(Ordering::Acquire)));\n",
                n, n
            ));
        }
        let s = format!(
            "
        pub fn prometheus(&self) -> String {{
            let mut ret = String::new();
            ret.push_str(&format!(\"daqingest_aggcount {{}}\\n\", self.aggcount.load(Ordering::Acquire)));
{buf}
            ret
        }}
    "
        );
        code.push_str(&s);
    }
    code.push_str(
        "
}
",
    );
    code
}

// TODO maybe basic and agg structs need a different treatment?
// Should probably implement the methods behind a common trait.
fn diff_decl_impl(st: &DiffStructDef, inp: &StatsStructDef) -> String {
    let name = &st.name;
    let inp_ty = &inp.name;
    let decl = inp
        .counters
        .iter()
        .map(|x| format!("{:4}pub {}: AtomicU64,\n", "", x.to_string()))
        .fold(String::new(), extend_str);
    let mut code = String::new();
    let s = format!(
        "
pub struct {name} {{
    pub dt: AtomicU64,
{decl}
}}
"
    );
    code.push_str(&s);
    code.push_str(&format!(
        "impl {name} {{
"
    ));
    let diffs = inp
        .counters
        .iter()
        .map(|x| {
            let n = x.to_string();
            format!(
                "{:12}let {} = AtomicU64::new(b.{}.load(Ordering::Acquire) - a.{}.load(Ordering::Acquire));\n",
                "", n, n, n
            )
        })
        .fold(String::new(), extend_str);
    let inits = inp
        .counters
        .iter()
        .map(|x| {
            let n = x.to_string();
            format!("{:16}{},\n", "", n)
        })
        .fold(String::new(), extend_str);
    let s = format!(
        "
        pub fn diff_from(a: &{inp_ty}, b: &{inp_ty}) -> Self {{
            let dur = b.ts_create.duration_since(a.ts_create);
{diffs}
            Self {{
                dt: AtomicU64::new(dur.as_secs() * SEC + dur.subsec_nanos() as u64),
{inits}
            }}
        }}
    "
    );
    code.push_str(&s);
    let mut a = String::new();
    let mut b = String::new();
    for h in &inp.counters {
        a.push_str(&format!("{} {{}}  ", h.to_string()));
        b.push_str(&format!("self.{}.load(Ordering::Acquire), ", h.to_string()));
    }
    let s = format!(
        "
    pub fn display(&self) -> String {{
        format!(\"dt {{}}  {a}\", self.dt.load(Ordering::Acquire) / 1000000, {b})
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

impl StatsStructDef {
    fn empty() -> Self {
        Self {
            name: syn::parse_str("__empty").unwrap(),
            prefix: syn::parse_str("__empty").unwrap(),
            counters: Vec::new(),
            values: Vec::new(),
        }
    }

    fn from_args(inp: PunctExpr) -> syn::Result<Self> {
        let mut name = None;
        let mut prefix = None;
        let mut counters = None;
        let mut values = None;
        for k in inp {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "name" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                name = Some(ident);
            } else if fa.name == "prefix" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                prefix = Some(ident);
            } else if fa.name == "counters" {
                let idents = idents_from_exprs(fa.args)?;
                counters = Some(idents);
            } else if fa.name == "values" {
                let idents = idents_from_exprs(fa.args)?;
                values = Some(idents);
            } else {
                panic!("fa.name: {:?}", fa.name);
            }
        }
        let ret = StatsStructDef {
            name: name.expect("Expect name for StatsStructDef"),
            prefix,
            counters: counters.unwrap_or(Vec::new()),
            values: values.unwrap_or(Vec::new()),
        };
        Ok(ret)
    }
}

impl AggStructDef {
    fn from_args(inp: PunctExpr) -> syn::Result<Self> {
        let mut name = None;
        let mut parent = None;
        for k in inp {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "name" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                name = Some(ident);
            }
            if fa.name == "parent" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                parent = Some(ident);
            }
        }
        let ret = AggStructDef {
            name: name.expect("Expect name for AggStructDef"),
            // Will get resolved later:
            stats: StatsStructDef::empty(),
            parent: parent.expect("Expect parent"),
        };
        Ok(ret)
    }
}

impl DiffStructDef {
    fn from_args(inp: PunctExpr) -> syn::Result<Self> {
        let mut name = None;
        let mut input = None;
        for k in inp {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "name" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                name = Some(ident);
            }
            if fa.name == "input" {
                let ident = ident_from_expr(fa.args[0].clone())?;
                input = Some(ident);
            }
        }
        let ret = DiffStructDef {
            name: name.expect("Expect name for DiffStructDef"),
            input: input.expect("Expect input for DiffStructDef"),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
struct StatsTreeDef {
    stats_struct_defs: Vec<StatsStructDef>,
    agg_defs: Vec<AggStructDef>,
    diff_defs: Vec<DiffStructDef>,
}

impl syn::parse::Parse for StatsTreeDef {
    fn parse(inp: ParseStream) -> syn::Result<Self> {
        let k = inp.parse::<syn::ExprTuple>()?;
        let mut a = vec![];
        let mut agg_defs = vec![];
        let mut diff_defs = vec![];
        for k in k.elems {
            let fa = FuncCallWithArgs::from_expr(k)?;
            if fa.name == "stats_struct" {
                let stats_struct_def = StatsStructDef::from_args(fa.args)?;
                a.push(stats_struct_def);
            } else if fa.name == "agg" {
                let agg_def = AggStructDef::from_args(fa.args)?;
                agg_defs.push(agg_def);
            } else if fa.name == "diff" {
                let diff_def = DiffStructDef::from_args(fa.args)?;
                diff_defs.push(diff_def);
            } else {
                return Err(syn::Error::new(fa.name.span(), "Unexpected"));
            }
        }
        let ret = StatsTreeDef {
            stats_struct_defs: a,
            agg_defs,
            diff_defs,
        };
        Ok(ret)
    }
}

#[proc_macro]
pub fn stats_struct(ts: TokenStream) -> TokenStream {
    let mut def: StatsTreeDef = parse_macro_input!(ts);
    for h in &mut def.agg_defs {
        for k in &def.stats_struct_defs {
            if k.name == h.parent {
                // TODO factor this out..
                h.stats = k.clone();
            }
        }
    }
    if false {
        for j in &def.agg_defs {
            let h = StatsStructDef {
                name: j.name.clone(),
                prefix: None,
                counters: j.stats.counters.clone(),
                values: Vec::new(),
            };
            def.stats_struct_defs.push(h);
        }
    }
    let mut code = String::new();
    let mut ts1 = TokenStream::new();
    for k in &def.stats_struct_defs {
        let s = stats_struct_decl_impl(k);
        code.push_str(&s);
        let ts2: TokenStream = s.parse().unwrap();
        ts1.extend(ts2);
    }
    for k in &def.agg_defs {
        for st in &def.stats_struct_defs {
            if st.name == k.parent {
                let s = agg_decl_impl(st, k);
                code.push_str(&s);
                let ts2: TokenStream = s.parse().unwrap();
                ts1.extend(ts2);
            }
        }
    }
    for k in &def.diff_defs {
        for j in &def.agg_defs {
            if j.name == k.input {
                // TODO currently, "j.stats" describes the input to the "agg", so that contains the wrong name.
                let p = StatsStructDef {
                    name: k.input.clone(),
                    // TODO refactor
                    prefix: None,
                    counters: j.stats.counters.clone(),
                    // TODO compute values
                    values: Vec::new(),
                };
                let s = diff_decl_impl(k, &p);
                code.push_str(&s);
                let ts2: TokenStream = s.parse().unwrap();
                ts1.extend(ts2);
            }
        }
        for j in &def.stats_struct_defs {
            if j.name == k.input {
                let s = diff_decl_impl(k, j);
                code.push_str(&s);
                let ts2: TokenStream = s.parse().unwrap();
                ts1.extend(ts2);
            }
        }
    }
    //panic!("CODE: {}", code);
    let _ts3 = TokenStream::from(quote!(
        mod asd {}
    ));
    ts1
}
