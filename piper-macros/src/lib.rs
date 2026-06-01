use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Expr, Ident, Result, Token, Type, Visibility, braced, bracketed, parenthesized,
    parse_macro_input,
};

struct PipelineInput {
    vis: Visibility,
    name: Ident,
    input: Type,
    output: Type,
    error: Type,
    config: Expr,
    nodes: NodeDecls,
    graph: Option<Vec<Edge>>,
}

enum NodeDecls {
    Linear(Vec<Expr>),
    Named(Vec<NamedNode>),
}

struct NamedNode {
    name: Ident,
    kind: NamedNodeKind,
}

enum NamedNodeKind {
    Managed(Expr),
    External { input: Type, output: Type },
}

struct GraphExpansion {
    build_graph: proc_macro2::TokenStream,
    external_nodes: Vec<ExternalNodeDecl>,
}

struct ExternalNodeDecl {
    name: Ident,
    input: Type,
    output: Type,
    token_ident: Ident,
}

#[derive(Clone)]
enum Endpoint {
    Input,
    Output,
    Node(Ident),
}

struct EndpointList {
    endpoints: Vec<Endpoint>,
}

struct Edge {
    from: EndpointList,
    to: EndpointList,
}

impl Parse for Endpoint {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "input" => Ok(Endpoint::Input),
            "output" => Ok(Endpoint::Output),
            _ => Ok(Endpoint::Node(ident)),
        }
    }
}

impl Parse for EndpointList {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        if input.peek(syn::token::Bracket) {
            let content;
            bracketed!(content in input);
            let endpoints = Punctuated::<Endpoint, Token![,]>::parse_terminated(&content)?;
            Ok(EndpointList {
                endpoints: endpoints.into_iter().collect(),
            })
        } else {
            Ok(EndpointList {
                endpoints: vec![input.parse()?],
            })
        }
    }
}

impl Parse for Edge {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let from = input.parse()?;
        input.parse::<Token![->]>()?;
        let to = input.parse()?;
        Ok(Edge { from, to })
    }
}

impl Parse for NamedNode {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let name = input.parse()?;
        input.parse::<Token![=]>()?;
        if input.peek(Ident) {
            let fork = input.fork();
            let ident: Ident = fork.parse()?;
            if ident == "external_node" && fork.peek(syn::token::Paren) {
                input.parse::<Ident>()?;
                let content;
                parenthesized!(content in input);
                let input_type = content.parse()?;
                content.parse::<Token![,]>()?;
                let output_type = content.parse()?;
                if !content.is_empty() {
                    return Err(content.error("expected exactly two external node types"));
                }
                return Ok(NamedNode {
                    name,
                    kind: NamedNodeKind::External {
                        input: input_type,
                        output: output_type,
                    },
                });
            }
        }
        let expr = input.parse()?;
        Ok(NamedNode {
            name,
            kind: NamedNodeKind::Managed(expr),
        })
    }
}

impl Parse for PipelineInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let vis = input.parse()?;
        input.parse::<Token![struct]>()?;
        let name = input.parse()?;

        let content;
        braced!(content in input);

        let mut pipeline_input = None;
        let mut output = None;
        let mut error = None;
        let mut config = None;
        let mut nodes = None;
        let mut graph = None;

        while !content.is_empty() {
            if content.peek(Token![type]) {
                content.parse::<Token![type]>()?;
                let key: Ident = content.parse()?;
                content.parse::<Token![=]>()?;
                let value: Type = content.parse()?;
                content.parse::<Token![;]>()?;

                match key.to_string().as_str() {
                    "Input" => pipeline_input = Some(value),
                    "Output" => output = Some(value),
                    "Error" => error = Some(value),
                    _ => {
                        return Err(syn::Error::new(
                            key.span(),
                            "expected Input, Output, or Error",
                        ));
                    }
                }
                continue;
            }

            let key: Ident = content.parse()?;
            content.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "config" => {
                    config = Some(content.parse()?);
                    content.parse::<Token![;]>()?;
                }
                "nodes" => {
                    if content.peek(syn::token::Bracket) {
                        let node_content;
                        bracketed!(node_content in content);
                        let parsed =
                            Punctuated::<Expr, Token![,]>::parse_terminated(&node_content)?;
                        nodes = Some(NodeDecls::Linear(parsed.into_iter().collect()));
                    } else {
                        let node_content;
                        braced!(node_content in content);
                        let parsed =
                            Punctuated::<NamedNode, Token![,]>::parse_terminated(&node_content)?;
                        nodes = Some(NodeDecls::Named(parsed.into_iter().collect()));
                    }
                    content.parse::<Token![;]>()?;
                }
                "graph" => {
                    let graph_content;
                    braced!(graph_content in content);
                    let mut edges = Vec::new();
                    while !graph_content.is_empty() {
                        edges.push(graph_content.parse()?);
                        if graph_content.is_empty() {
                            break;
                        }
                        graph_content.parse::<Token![;]>()?;
                    }
                    graph = Some(edges);
                    content.parse::<Token![;]>()?;
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        "expected config, nodes, or graph assignment",
                    ));
                }
            }
        }

        Ok(PipelineInput {
            vis,
            name,
            input: pipeline_input
                .ok_or_else(|| syn::Error::new(content.span(), "missing `type Input = ...;`"))?,
            output: output
                .ok_or_else(|| syn::Error::new(content.span(), "missing `type Output = ...;`"))?,
            error: error
                .ok_or_else(|| syn::Error::new(content.span(), "missing `type Error = ...;`"))?,
            config: config
                .ok_or_else(|| syn::Error::new(content.span(), "missing `config = ...;`"))?,
            nodes: nodes
                .ok_or_else(|| syn::Error::new(content.span(), "missing `nodes = ...;`"))?,
            graph,
        })
    }
}

#[proc_macro]
pub fn pipeline(input: TokenStream) -> TokenStream {
    let PipelineInput {
        vis,
        name,
        input,
        output,
        error,
        config,
        nodes,
        graph,
    } = parse_macro_input!(input as PipelineInput);

    let expansion = match (nodes, graph) {
        (NodeDecls::Linear(nodes), None) => expand_linear_graph(&input, &output, &error, nodes),
        (NodeDecls::Named(nodes), Some(edges)) => {
            match expand_named_graph(&input, &output, &error, nodes, edges) {
                Ok(tokens) => tokens,
                Err(error) => return error.to_compile_error().into(),
            }
        }
        (NodeDecls::Linear(_), Some(_)) => {
            return syn::Error::new_spanned(
                name,
                "`graph = ...` requires named `nodes = { ... };`",
            )
            .to_compile_error()
            .into();
        }
        (NodeDecls::Named(_), None) => {
            return syn::Error::new_spanned(name, "named nodes require `graph = { ... };`")
                .to_compile_error()
                .into();
        }
    };

    let build_graph = expansion.build_graph;

    if expansion.external_nodes.is_empty() {
        quote! {
            #vis struct #name;

            impl #name {
                pub fn start() -> ::piper::Result<::piper::Piper<#input, #output, #error>, #error> {
                    #build_graph
                    ::piper::Piper::start(#config, __piper_graph)
                }
            }
        }
        .into()
    } else {
        let run_name = format_ident!("{name}Run");
        let external_fields = expansion.external_nodes.iter().map(|external| {
            let field = &external.name;
            let input = &external.input;
            let output = &external.output;
            quote! {
                pub #field: ::piper::ExternalNode<#input, #output, #error>
            }
        });
        let external_takes = expansion.external_nodes.iter().map(|external| {
            let field = &external.name;
            let token = &external.token_ident;
            quote! {
                let #field = __piper.take_external_node(#token);
            }
        });
        let external_names: Vec<_> = expansion
            .external_nodes
            .iter()
            .map(|external| &external.name)
            .collect();

        quote! {
            #vis struct #name;

            #vis struct #run_name {
                pub piper: ::piper::Piper<#input, #output, #error>,
                #(#external_fields,)*
            }

            impl #name {
                pub fn start() -> ::piper::Result<#run_name, #error> {
                    #build_graph
                    let mut __piper = ::piper::Piper::start(#config, __piper_graph)?;
                    #(#external_takes)*
                    Ok(#run_name {
                        piper: __piper,
                        #(#external_names,)*
                    })
                }
            }

            impl #run_name {
                pub fn sender(&self) -> ::piper::PiperSender<#input> {
                    self.piper.sender()
                }

                pub fn receiver(&self) -> ::piper::PiperReceiver<#output> {
                    self.piper.receiver()
                }

                pub fn shutdown(&self) {
                    self.piper.shutdown();
                }

                pub fn abort(&self) {
                    self.piper.abort();
                }

                pub fn get_telemetry(&self) -> ::piper::PiperSnapshot {
                    self.piper.get_telemetry()
                }

                pub fn join(self) -> ::piper::Result<(), #error> {
                    let #run_name {
                        piper,
                        #(#external_names,)*
                    } = self;
                    drop((#(#external_names,)*));
                    piper.join()
                }
            }
        }
        .into()
    }
}

fn expand_linear_graph(
    input: &Type,
    output: &Type,
    error: &Type,
    nodes: Vec<Expr>,
) -> GraphExpansion {
    let mut tokens = quote! {
        let mut __piper_builder = ::piper::PipelineGraphBuilder::<#input, #error>::new();
        let __piper_link_0 = __piper_builder.input();
    };
    let mut previous = format_ident!("__piper_link_0");
    for (index, node) in nodes.into_iter().enumerate() {
        let next = format_ident!("__piper_link_{}", index + 1);
        tokens.extend(quote! {
            let #next = __piper_builder.add_node(#previous, #node);
        });
        previous = next;
    }
    tokens.extend(quote! {
        let __piper_graph = __piper_builder.finish::<#output>(#previous);
    });
    GraphExpansion {
        build_graph: tokens,
        external_nodes: Vec::new(),
    }
}

fn expand_named_graph(
    input: &Type,
    output: &Type,
    error: &Type,
    nodes: Vec<NamedNode>,
    edges: Vec<Edge>,
) -> Result<GraphExpansion> {
    let declared: HashSet<String> = nodes.iter().map(|node| node.name.to_string()).collect();
    let mut parent = HashMap::<String, String>::new();
    let mut used_inputs = HashSet::<String>::new();
    let mut used_outputs = HashSet::<String>::new();
    let mut adjacency = HashMap::<String, Vec<String>>::new();

    insert_key(&mut parent, "input:out");
    insert_key(&mut parent, "output:in");
    for node in &nodes {
        insert_key(&mut parent, &format!("{}:in", node.name));
        insert_key(&mut parent, &format!("{}:out", node.name));
    }

    for edge in &edges {
        for from in &edge.from.endpoints {
            validate_endpoint(from, &declared)?;
            let from_key = source_key(from)?;
            used_outputs.insert(from_key.clone());
            for to in &edge.to.endpoints {
                validate_endpoint(to, &declared)?;
                let to_key = dest_key(to)?;
                used_inputs.insert(to_key.clone());
                union(&mut parent, &from_key, &to_key);
                if let (Endpoint::Node(from_node), Endpoint::Node(to_node)) = (from, to) {
                    adjacency
                        .entry(from_node.to_string())
                        .or_default()
                        .push(to_node.to_string());
                }
            }
        }
    }

    if !used_outputs.contains("input:out") {
        return Err(syn::Error::new_spanned(
            &nodes[0].name,
            "graph must connect `input` to at least one node",
        ));
    }
    if !used_inputs.contains("output:in") {
        return Err(syn::Error::new_spanned(
            &nodes[0].name,
            "graph must connect at least one node to `output`",
        ));
    }
    for node in &nodes {
        let input_key = format!("{}:in", node.name);
        let output_key = format!("{}:out", node.name);
        if !used_inputs.contains(&input_key) {
            return Err(syn::Error::new_spanned(
                &node.name,
                "node is missing an input graph edge",
            ));
        }
        if !used_outputs.contains(&output_key) {
            return Err(syn::Error::new_spanned(
                &node.name,
                "node is missing an output graph edge",
            ));
        }
    }
    detect_cycles(&nodes, &adjacency)?;

    let mut root_to_ident = BTreeMap::<String, Ident>::new();
    let mut roots = BTreeSet::new();
    let keys: Vec<_> = parent.keys().cloned().collect();
    for key in keys {
        roots.insert(find(&mut parent, &key));
    }
    for (index, root) in roots.into_iter().enumerate() {
        root_to_ident.insert(root, format_ident!("__piper_link_{index}"));
    }

    let input_root = find(&mut parent, "input:out");
    let output_root = find(&mut parent, "output:in");
    let input_link = root_to_ident.get(&input_root).expect("input root exists");
    let output_link = root_to_ident.get(&output_root).expect("output root exists");

    let mut link_decls = quote! {};
    for (root, ident) in &root_to_ident {
        if root == &input_root {
            link_decls.extend(quote! {
                let #ident = __piper_builder.input();
            });
        } else {
            link_decls.extend(quote! {
                let #ident = __piper_builder.link();
            });
        }
    }

    let mut node_decls = quote! {};
    let mut external_nodes = Vec::new();
    for node in nodes {
        let name = node.name;
        let in_root = find(&mut parent, &format!("{name}:in"));
        let out_root = find(&mut parent, &format!("{name}:out"));
        let in_link = root_to_ident
            .get(&in_root)
            .expect("node input root exists");
        let out_link = root_to_ident
            .get(&out_root)
            .expect("node output root exists");
        match node.kind {
            NamedNodeKind::Managed(expr) => {
                node_decls.extend(quote! {
                    let #name = #expr;
                    __piper_builder.add_node_to(#in_link, #name, #out_link);
                });
            }
            NamedNodeKind::External { input, output } => {
                let token_ident = format_ident!("__piper_external_{name}");
                node_decls.extend(quote! {
                    let #token_ident = __piper_builder
                        .add_external_node_to::<#input, #output>(#in_link, stringify!(#name), #out_link);
                });
                external_nodes.push(ExternalNodeDecl {
                    name,
                    input,
                    output,
                    token_ident,
                });
            }
        }
    }

    Ok(GraphExpansion {
        build_graph: quote! {
            let mut __piper_builder = ::piper::PipelineGraphBuilder::<#input, #error>::new();
            #link_decls
            #node_decls
            let __piper_graph = __piper_builder.finish::<#output>(#output_link);
            let _ = #input_link;
        },
        external_nodes,
    })
}

fn validate_endpoint(endpoint: &Endpoint, declared: &HashSet<String>) -> Result<()> {
    if let Endpoint::Node(node) = endpoint {
        if !declared.contains(&node.to_string()) {
            return Err(syn::Error::new_spanned(node, "unknown graph node"));
        }
    }
    Ok(())
}

fn source_key(endpoint: &Endpoint) -> Result<String> {
    match endpoint {
        Endpoint::Input => Ok("input:out".to_string()),
        Endpoint::Output => Err(syn::Error::new_spanned(
            quote!(output),
            "`output` cannot be used as a graph edge source",
        )),
        Endpoint::Node(node) => Ok(format!("{node}:out")),
    }
}

fn dest_key(endpoint: &Endpoint) -> Result<String> {
    match endpoint {
        Endpoint::Input => Err(syn::Error::new_spanned(
            quote!(input),
            "`input` cannot be used as a graph edge destination",
        )),
        Endpoint::Output => Ok("output:in".to_string()),
        Endpoint::Node(node) => Ok(format!("{node}:in")),
    }
}

fn insert_key(parent: &mut HashMap<String, String>, key: &str) {
    parent.insert(key.to_string(), key.to_string());
}

fn find(parent: &mut HashMap<String, String>, key: &str) -> String {
    let current = parent.get(key).cloned().unwrap_or_else(|| key.to_string());
    if current == key {
        current
    } else {
        let root = find(parent, &current);
        parent.insert(key.to_string(), root.clone());
        root
    }
}

fn union(parent: &mut HashMap<String, String>, left: &str, right: &str) {
    let left_root = find(parent, left);
    let right_root = find(parent, right);
    if left_root != right_root {
        parent.insert(right_root, left_root);
    }
}

fn detect_cycles(nodes: &[NamedNode], adjacency: &HashMap<String, Vec<String>>) -> Result<()> {
    fn visit(
        node: &str,
        adjacency: &HashMap<String, Vec<String>>,
        temporary: &mut HashSet<String>,
        permanent: &mut HashSet<String>,
    ) -> bool {
        if permanent.contains(node) {
            return false;
        }
        if !temporary.insert(node.to_string()) {
            return true;
        }
        if let Some(next) = adjacency.get(node) {
            for child in next {
                if visit(child, adjacency, temporary, permanent) {
                    return true;
                }
            }
        }
        temporary.remove(node);
        permanent.insert(node.to_string());
        false
    }

    let mut temporary = HashSet::new();
    let mut permanent = HashSet::new();
    for node in nodes {
        if visit(
            &node.name.to_string(),
            adjacency,
            &mut temporary,
            &mut permanent,
        ) {
            return Err(syn::Error::new_spanned(
                &node.name,
                "graph cycles are not supported",
            ));
        }
    }
    Ok(())
}
