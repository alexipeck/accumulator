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
    return_state: Option<Type>,
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
    External {
        input: Type,
        output: Type,
        reusable_factory: Option<Expr>,
    },
}

struct GraphExpansion {
    build_graph: proc_macro2::TokenStream,
    external_nodes: Vec<ExternalNodeDecl>,
    return_state: Option<ReturnStateDecl>,
}

struct ReturnStateDecl {
    state: Type,
    mergeable: bool,
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
                let reusable_factory = if input.peek(Token![.]) {
                    input.parse::<Token![.]>()?;
                    let method: Ident = input.parse()?;
                    if method != "with_reusable_output" {
                        return Err(syn::Error::new(
                            method.span(),
                            "external nodes only support `.with_reusable_output(factory)`",
                        ));
                    }
                    let factory_content;
                    parenthesized!(factory_content in input);
                    let factory = factory_content.parse::<Expr>()?;
                    if factory_content.peek(Token![,]) {
                        factory_content.parse::<Token![,]>()?;
                    }
                    if !factory_content.is_empty() {
                        return Err(factory_content.error("unexpected tokens in with_reusable_output"));
                    }
                    if input.peek(Token![.]) {
                        return Err(syn::Error::new(
                            input.span(),
                            "external nodes support at most one `.with_reusable_output(...)` chain",
                        ));
                    }
                    Some(factory)
                } else {
                    None
                };
                return Ok(NamedNode {
                    name,
                    kind: NamedNodeKind::External {
                        input: input_type,
                        output: output_type,
                        reusable_factory,
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
        let mut return_state = None;

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
                "return_state" => {
                    return_state = Some(content.parse()?);
                    content.parse::<Token![;]>()?;
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        "expected config, nodes, graph, or return_state assignment",
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
            return_state,
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
        return_state,
    } = parse_macro_input!(input as PipelineInput);

    let expansion = match (nodes, graph) {
        (NodeDecls::Linear(nodes), None) => {
            expand_linear_graph(&input, &output, &error, nodes, return_state)
        }
        (NodeDecls::Named(nodes), Some(edges)) => {
            match expand_named_graph(&input, &output, &error, nodes, edges, return_state) {
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
    let return_state = expansion.return_state;

    if let Some(return_state) = return_state {
        let state = return_state.state;
        let mergeable = return_state.mergeable;
        let merge_const = if mergeable {
            quote!(true)
        } else {
            quote!(false)
        };
        if expansion.external_nodes.is_empty() {
            quote! {
                #vis struct #name;

                impl #name {
                    pub fn start() -> ::piper::Result<::piper::PiperWithState<#input, #output, #state, #error, #merge_const>, #error> {
                        #build_graph
                        ::piper::PiperWithState::start(#config, __piper_graph)
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
            let join_merged_method = if mergeable {
                quote! {
                    pub fn join_merged(self) -> ::piper::Result<#state, #error> {
                        let #run_name {
                            piper,
                            #(#external_names,)*
                        } = self;
                        drop((#(#external_names,)*));
                        piper.join_merged()
                    }
                }
            } else {
                quote! {}
            };

            quote! {
                #vis struct #name;

                #vis struct #run_name {
                    pub piper: ::piper::PiperWithState<#input, #output, #state, #error, #merge_const>,
                    #(#external_fields,)*
                }

                impl #name {
                    pub fn start() -> ::piper::Result<#run_name, #error> {
                        #build_graph
                        let mut __piper = ::piper::PiperWithState::start(#config, __piper_graph)?;
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

                    pub fn join(self) -> ::piper::Result<::std::vec::Vec<#state>, #error> {
                        let #run_name {
                            piper,
                            #(#external_names,)*
                        } = self;
                        drop((#(#external_names,)*));
                        piper.join()
                    }

                    #join_merged_method
                }
            }
            .into()
        }
    } else if expansion.external_nodes.is_empty() {
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
    return_state: Option<Type>,
) -> GraphExpansion {
    let return_state_decl = return_state.map(|state| ReturnStateDecl {
        mergeable: nodes
            .last()
            .is_some_and(expr_declares_state_merge),
        state,
    });
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
    if let Some(return_state) = &return_state_decl {
        let state = &return_state.state;
        if return_state.mergeable {
            tokens.extend(quote! {
                let __piper_graph = __piper_builder.finish_with_merged_state::<#output, #state>(#previous);
            });
        } else {
            tokens.extend(quote! {
                let __piper_graph = __piper_builder.finish_with_state::<#output, #state>(#previous);
            });
        }
    } else {
        tokens.extend(quote! {
            let __piper_graph = __piper_builder.finish::<#output>(#previous);
        });
    }
    GraphExpansion {
        build_graph: tokens,
        external_nodes: Vec::new(),
        return_state: return_state_decl,
    }
}

fn expand_named_graph(
    input: &Type,
    output: &Type,
    error: &Type,
    nodes: Vec<NamedNode>,
    edges: Vec<Edge>,
    return_state: Option<Type>,
) -> Result<GraphExpansion> {
    let return_state_decl = match return_state {
        Some(state) => Some(ReturnStateDecl {
            mergeable: named_return_state_mergeable(&nodes, &edges)?,
            state,
        }),
        None => None,
    };
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
            NamedNodeKind::External {
                input,
                output,
                reusable_factory,
            } => {
                let token_ident = format_ident!("__piper_external_{name}");
                match reusable_factory {
                    Some(factory) => {
                        node_decls.extend(quote! {
                            let #token_ident = __piper_builder
                                .add_external_node_to_with_reusable_output::<#input, #output, _, _>(
                                    #in_link,
                                    stringify!(#name),
                                    #out_link,
                                    #factory,
                                );
                        });
                    }
                    None => {
                        node_decls.extend(quote! {
                            let #token_ident = __piper_builder
                                .add_external_node_to::<#input, #output>(
                                    #in_link,
                                    stringify!(#name),
                                    #out_link,
                                );
                        });
                    }
                }
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
        build_graph: {
            let finish = if let Some(return_state) = &return_state_decl {
                let state = &return_state.state;
                if return_state.mergeable {
                    quote! {
                        let __piper_graph = __piper_builder.finish_with_merged_state::<#output, #state>(#output_link);
                    }
                } else {
                    quote! {
                        let __piper_graph = __piper_builder.finish_with_state::<#output, #state>(#output_link);
                    }
                }
            } else {
                quote! {
                    let __piper_graph = __piper_builder.finish::<#output>(#output_link);
                }
            };
            quote! {
                let mut __piper_builder = ::piper::PipelineGraphBuilder::<#input, #error>::new();
                #link_decls
                #node_decls
                #finish
                let _ = #input_link;
            }
        },
        external_nodes,
        return_state: return_state_decl,
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

fn named_return_state_mergeable(nodes: &[NamedNode], edges: &[Edge]) -> Result<bool> {
    let mut producers = Vec::new();
    for edge in edges {
        if edge
            .to
            .endpoints
            .iter()
            .any(|endpoint| matches!(endpoint, Endpoint::Output))
        {
            producers.extend(edge.from.endpoints.iter());
        }
    }

    if producers.len() != 1 {
        let span = nodes
            .first()
            .map(|node| node.name.span())
            .unwrap_or(proc_macro2::Span::call_site());
        return Err(syn::Error::new(
            span,
            "return_state requires exactly one managed node to feed `output`",
        ));
    }

    let Endpoint::Node(final_node) = producers[0] else {
        return Err(syn::Error::new_spanned(
            quote!(return_state),
            "return_state requires the final output producer to be a managed node",
        ));
    };
    let Some(node) = nodes
        .iter()
        .find(|node| node.name == *final_node)
    else {
        return Err(syn::Error::new_spanned(final_node, "unknown graph node"));
    };
    match &node.kind {
        NamedNodeKind::Managed(expr) => Ok(expr_declares_state_merge(expr)),
        NamedNodeKind::External { .. } => Err(syn::Error::new_spanned(
            final_node,
            "return_state requires the final output producer to be a managed node",
        )),
    }
}

fn expr_declares_state_merge(expr: &Expr) -> bool {
    match expr {
        Expr::Call(call) => {
            expr_path_last_ident(&call.func)
                .is_some_and(|ident| ident == "node_with_state_merge")
                || expr_declares_state_merge(&call.func)
                || call.args.iter().any(expr_declares_state_merge)
        }
        Expr::MethodCall(method) => expr_declares_state_merge(&method.receiver),
        Expr::Paren(paren) => expr_declares_state_merge(&paren.expr),
        Expr::Group(group) => expr_declares_state_merge(&group.expr),
        Expr::Reference(reference) => expr_declares_state_merge(&reference.expr),
        Expr::Try(expr_try) => expr_declares_state_merge(&expr_try.expr),
        _ => false,
    }
}

fn expr_path_last_ident(expr: &Expr) -> Option<String> {
    let Expr::Path(path) = expr else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}
