// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! Support for compiling scripts and modules in tests.

use anyhow::{anyhow, Result};
use aptos_types::{
    account_address::AccountAddress,
    transaction::{Module, Script},
};
use aptos_framework::named_addresses;
use move_binary_format::CompiledModule;
use move_command_line_common::{
    self, address::NumericalAddress, env::read_bool_env_var, parser::NumberFormat,
};
use move_compiler::{
    compiled_unit::AnnotatedCompiledUnit,
    diagnostics::{Diagnostics, FilesSourceText},
    shared::PackagePaths,
    FullyCompiledProgram,
};
use move_core_types::language_storage::ModuleId;
use move_ir_compiler::Compiler;

use once_cell::sync::Lazy;

use std;
use std::collections::{BTreeMap, HashMap};
use aptos_cached_packages::head_release_bundle;

static PRECOMPILED_APTOS_FRAMEWORK: Lazy<FullyCompiledProgram> = Lazy::new(|| {
    let deps = vec![PackagePaths {
        name: None,
        paths: head_release_bundle().files().unwrap(),
        named_address_map: named_addresses().clone(),
    }];
    let program_res = move_compiler::construct_pre_compiled_lib(
        deps,
        None,
        move_compiler::Flags::empty().set_sources_shadow_deps(false),
    )
    .unwrap();
    match program_res {
        Ok(af) => af,
        Err((files, errors)) => {
            eprintln!("!!!Aptos Framework failed to compile!!!");
            move_compiler::diagnostics::report_diagnostics(&files, errors)
        }
    }
});

fn compile_source_unit(
    pre_compiled_deps: Option<&FullyCompiledProgram>,
    named_address_mapping: BTreeMap<String, NumericalAddress>,
    deps: &[String],
    path: String,
) -> Result<(AnnotatedCompiledUnit, Option<String>)> {
    fn rendered_diags(files: &FilesSourceText, diags: Diagnostics) -> Option<String> {
        if diags.is_empty() {
            return None;
        }

        let error_buffer = if read_bool_env_var(move_command_line_common::testing::PRETTY) {
            move_compiler::diagnostics::report_diagnostics_to_color_buffer(files, diags)
        } else {
            move_compiler::diagnostics::report_diagnostics_to_buffer(files, diags)
        };
        Some(String::from_utf8(error_buffer).unwrap())
    }

    use move_compiler::PASS_COMPILATION;
    let (mut files, comments_and_compiler_res) =
        move_compiler::Compiler::from_files(vec![path], deps.to_vec(), named_address_mapping)
            .set_pre_compiled_lib_opt(pre_compiled_deps)
            .set_flags(move_compiler::Flags::empty().set_sources_shadow_deps(true))
            .run::<PASS_COMPILATION>()?;
    let units_or_diags = comments_and_compiler_res
        .map(|(_comments, move_compiler)| move_compiler.into_compiled_units());

    match units_or_diags {
        Err(diags) => {
            if let Some(pcd) = pre_compiled_deps {
                for (file_name, text) in &pcd.files {
                    // TODO This is bad. Rethink this when errors are redone
                    if !files.contains_key(file_name) {
                        files.insert(*file_name, text.clone());
                    }
                }
            }

            Err(anyhow!(rendered_diags(&files, diags).unwrap()))
        }
        Ok((mut units, warnings)) => {
            let warnings = rendered_diags(&files, warnings);
            let len = units.len();
            if len != 1 {
                panic!("Invalid input. Expected 1 compiled unit but got {}", len)
            }
            let unit = units.pop().unwrap();
            Ok((unit, warnings))
        }
    }
}

pub fn compile_source_module(
    path: String,
    user_named_addresses: &HashMap<String, &AccountAddress>,
) -> (ModuleId, Module) {
    let mut named_address_mapping = named_addresses().clone();
    for (name, addr) in user_named_addresses {
        let addr = NumericalAddress::new(addr.into_bytes(), NumberFormat::Hex);
        if named_address_mapping.contains_key(name) {
            panic!(
                "Invalid init. The named address '{}' is reserved by the move-stdlib",
                name
            )
        }
        named_address_mapping.insert(name.to_string(), addr);
    }

    let unit = compile_source_unit(
        Some(&*PRECOMPILED_APTOS_FRAMEWORK),
        named_address_mapping,
        &Vec::new(),
        path,
    )
    .unwrap()
    .0;

    let (mid, module) = match unit {
        AnnotatedCompiledUnit::Module(annot_module) => {
            let (_named_addr_opt, mid) = annot_module.module_id();
            (mid, annot_module.named_module.module)
        }
        AnnotatedCompiledUnit::Script(_) => panic!("Expected a module text block, not a script"),
    };

    let mut serialized_module = Vec::<u8>::new();
    module.serialize(&mut serialized_module).unwrap();

    (mid, Module::new(serialized_module))
}

/// Compile the provided Move code into a blob which can be used as the code to be published
/// (a Module).
pub fn compile_module(code: &str) -> (CompiledModule, Module) {
    let framework_modules = aptos_cached_packages::head_release_bundle().compiled_modules();
    let compiled_module = Compiler {
        deps: framework_modules.iter().collect(),
    }
    .into_compiled_module(code)
    .expect("Module compilation failed");
    let module = Module::new(
        Compiler {
            deps: framework_modules.iter().collect(),
        }
        .into_module_blob(code)
        .expect("Module compilation failed"),
    );
    (compiled_module, module)
}

/// Compile the provided Move code into a blob which can be used as the code to be executed
/// (a Script).
pub fn compile_script(code: &str, mut extra_deps: Vec<CompiledModule>) -> Script {
    let mut framework_modules = aptos_cached_packages::head_release_bundle().compiled_modules();
    framework_modules.append(&mut extra_deps);
    let compiler = Compiler {
        deps: framework_modules.iter().collect(),
    };
    Script::new(
        compiler
            .into_script_blob(code)
            .expect("Script compilation failed"),
        vec![],
        vec![],
    )
}
