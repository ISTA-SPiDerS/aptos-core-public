// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
#![allow(unused)]

use crate::transaction_generator::publishing::raw_module_data;
use aptos_framework::natives::code::PackageMetadata;
use aptos_sdk::{
    bcs,
    move_types::{
        account_address::AccountAddress, ident_str, identifier::Identifier,
        language_storage::ModuleId,
    },
    types::{transaction::{EntryFunction, TransactionPayload}, LocalAccount},
};
use move_binary_format::{
    file_format::{FunctionHandleIndex, IdentifierIndex, SignatureToken},
    CompiledModule,
};
use rand::{distributions::Alphanumeric, prelude::StdRng, seq::SliceRandom, Rng};
use rand_core::RngCore;
use aptos_sdk::move_types::language_storage::TypeTag;
use aptos_sdk::move_types::language_storage::StructTag;
use rand::distributions::WeightedIndex;
use std::{thread, time};
use rand::prelude::*;
use aptos_sdk::types::account_config;

// use rand::distributions::Distribution;

// use aptos_language_e2e_tests::account_activity_distribution::{COIN_DISTR, TX_FROM, TX_NFT_FROM, TX_NFT_TO, TX_TO};
// use aptos_language_e2e_tests::solana_distribution::{RES_DISTR, COST_DISTR, LEN_DISTR};

//
// Contains all the code to work on the Simple package


// To get the full distribution, put the first element in the tuple, second element times into an array.



//
// Functions to load and update the original package
//

pub fn load_package() -> (Vec<CompiledModule>, PackageMetadata) {
    let metadata = bcs::from_bytes::<PackageMetadata>(&raw_module_data::PACKAGE_METADATA_SIMPLE)
        .expect("PackageMetadata for GenericModule must deserialize");
    let mut modules = vec![];
    let module = CompiledModule::deserialize(&raw_module_data::MODULE_SIMPLE)
        .expect("Simple.move must deserialize");
    modules.push(module);
    (modules, metadata)
}

pub fn version(module: &mut CompiledModule, rng: &mut StdRng) {
    // change `const COUNTER_STEP` in Simple.move
    // That is the only u64 in the constant pool
    for constant in &mut module.constant_pool {
        if constant.type_ == SignatureToken::U64 {
            let mut v: u64 = bcs::from_bytes(&constant.data).expect("U64 must deserialize");
            v += 1;
            constant.data = bcs::to_bytes(&v).expect("U64 must serialize");
            break;
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum LoadType {
    DEXAVG,
    DEXBURSTY,
    P2PTX,
    SOLANA,
    NFT
}


//
// List of entry points to expose
//
// More info in the Simple.move
#[derive(Debug, Copy, Clone)]
pub enum EntryPoints {
    /// Double the size of `Resource`
    Double,
    /// Half the size of `Resource`
    Half,
    // 1 arg
    /// run a for loop
    Loopy {
        loop_count: Option<u64>,
    },
    /// Return value from constant array (RANDOM)
    GetFromConst {
        const_idx: Option<u64>,
    },
    /// Set the `Resource.id`
    SetId,
    /// Set the `Resource.name`
    SetName,
    // 2 args
    // next 2 functions, second arg must be existing account address with data
    // Sets `Resource` to the max from two addresses
    Maximize,
    // Sets `Resource` to the min from two addresses
    Minimize,
    // 3 args
    /// Explicitly change Resource
    MakeOrChange {
        string_length: Option<usize>,
        data_length: Option<usize>,
    },
    BytesMakeOrChange {
        data_length: Option<usize>,
    },
}

impl EntryPoints {
    pub fn create_payload(
        &self,
        module_id: ModuleId,
        rng: Option<&mut StdRng>,
        other: Option<AccountAddress>,
        account: &mut LocalAccount,
        coin_num: usize,
        length: usize,
        writes: Vec<u64>,
    ) -> TransactionPayload {
        match self {
            // 0 args
            //This nop is never used if implementation works as planned
            EntryPoints::Double => get_payload_void(module_id, ident_str!("double").to_owned()),
            EntryPoints::Half => get_payload_void(module_id, ident_str!("half").to_owned()),
            // 1 arg
            EntryPoints::Loopy { loop_count } => get_payload_void(module_id, ident_str!("step").to_owned()),
            EntryPoints::GetFromConst { const_idx } => get_from_random_const(
                module_id,
                const_idx.unwrap_or_else(
                    // TODO: get a value in range for the const array in Simple.move
                    || rng.expect("Must provide RNG").gen_range(0u64, 1u64),
                ),
            ),
            EntryPoints::SetId => set_id(rng.expect("Must provide RNG"), module_id),
            EntryPoints::SetName => set_name(rng.expect("Must provide RNG"), module_id),
            // 2 args, second arg existing account address with data
            EntryPoints::Maximize => maximize(module_id, other.expect("Must provide other")),
            EntryPoints::Minimize => minimize(module_id, other.expect("Must provide other")),
            // 3 args
            EntryPoints::MakeOrChange {
                string_length,
                data_length,
            } => {
                let rng = rng.expect("Must provide RNG");
                let str_len = string_length.unwrap_or_else(|| rng.gen_range(0usize, 100usize));
                let data_len = data_length.unwrap_or_else(|| rng.gen_range(0usize, 1000usize));
                make_or_change(rng, module_id, str_len, data_len)
            },
            EntryPoints::BytesMakeOrChange { data_length } => {
                let rng = rng.expect("Must provide RNG");
                let data_len = data_length.unwrap_or_else(|| rng.gen_range(0usize, 1000usize));
                bytes_make_or_change(rng, module_id, data_len)
            },
        }
    }

}

// const ZERO_ARG_ENTRY_POINTS: &[EntryPoints; 6] = &[
//     EntryPoints::DEXAVG,
//     EntryPoints::Step,
//     EntryPoints::GetCounter,
//     EntryPoints::ResetData,
//     EntryPoints::Double,
//     EntryPoints::Half,
// ];
// const ONE_ARG_ENTRY_POINTS: &[EntryPoints; 4] = &[
//     EntryPoints::Loopy { loop_count: None },
//     EntryPoints::GetFromConst { const_idx: None },
//     EntryPoints::SetId,
//     EntryPoints::SetName,
// ];
// const SIMPLE_ENTRY_POINTS: &[EntryPoints; 9] = &[
//     EntryPoints::DEXAVG,
//     EntryPoints::Step,
//     EntryPoints::GetCounter,
//     EntryPoints::ResetData,
//     EntryPoints::Double,
//     EntryPoints::Half,
//     EntryPoints::Loopy { loop_count: None },
//     EntryPoints::GetFromConst { const_idx: None },
//     EntryPoints::SetId,
// ];
// const GEN_ENTRY_POINTS: &[EntryPoints; 12] = &[
//     EntryPoints::DEXAVG,
//     EntryPoints::Step,
//     EntryPoints::GetCounter,
//     EntryPoints::ResetData,
//     EntryPoints::Double,
//     EntryPoints::Half,
//     EntryPoints::Loopy { loop_count: None },
//     EntryPoints::GetFromConst { const_idx: None },
//     EntryPoints::SetId,
//     EntryPoints::SetName,
//     EntryPoints::MakeOrChange {
//         string_length: None,
//         data_length: None,
//     },
//     EntryPoints::BytesMakeOrChange { data_length: None },
// ];


pub fn dex_nft_payload(rng: Option<&mut StdRng>, module_id: ModuleId, account: &mut LocalAccount, coin_num: usize) -> TransactionPayload {
    let coin_1_num = coin_num;
    let coin_2_num: usize = coin_1_num;

    let mut coin: String = "CoinC".to_string();
    let mut coin_clone = coin.clone();

    let coin_number1_string = coin_1_num.to_string();
    let coin_number2_string = coin_2_num.to_string();
    coin.push_str(&coin_number1_string);
    coin_clone.push_str(&coin_number2_string);
    // println!("Coin1:{}", coin);
    // println!("Coin2:{}", coin_clone);

    //let coin_clone: &str = &coin.clone();
    let coin_id1 = Identifier::new(coin).unwrap();
    let coin_id2 = Identifier::new(coin_clone).unwrap();

    let coin_1 = TypeTag::Struct(Box::new(StructTag {
        address: account_config::CORE_CODE_ADDRESS,
        module: ident_str!("Benchmark").to_owned(),
        name: coin_id1,
        type_params: vec![],
    }));

    let coin_2 = TypeTag::Struct(Box::new(StructTag {
        address: account_config::CORE_CODE_ADDRESS,
        module: ident_str!("Benchmark").to_owned(),
        name: coin_id2,
        type_params: vec![],
    }));

    let entry_function = EntryFunction::new(
        ModuleId::new(
            account_config::CORE_CODE_ADDRESS,
            ident_str!("Benchmark").to_owned(),
        ),
        ident_str!("exchange").to_owned(),
        vec![coin_1.clone(), coin_2.clone()],
        vec![],
    );
    TransactionPayload::EntryFunction(entry_function)
}


pub fn sol_payload(rng: Option<&mut StdRng>, module_id: ModuleId, account: &mut LocalAccount, length: usize, writes: Vec<u64>) -> TransactionPayload {
    let entry_function = EntryFunction::new(
        ModuleId::new(
            account_config::CORE_CODE_ADDRESS,
            ident_str!("Benchmark").to_owned(),
        ),
        ident_str!("loop_exchange").to_owned(),
        vec![],
        vec![bcs::to_bytes(&length).unwrap(), bcs::to_bytes(&writes).unwrap()],
    );
    TransactionPayload::EntryFunction(entry_function)
}
// pub fn rand_simple_function(rng: &mut StdRng, module_id: ModuleId) -> TransactionPayload {
//     SIMPLE_ENTRY_POINTS
//         .choose(rng)
//         .unwrap()
//         .create_payload(module_id, Some(rng), None)
// }

// pub fn zero_args_function(rng: &mut StdRng, module_id: ModuleId) -> TransactionPayload {
//     ZERO_ARG_ENTRY_POINTS
//         .choose(rng)
//         .unwrap()
//         .create_payload(module_id, Some(rng), None)
// }

// pub fn rand_gen_function(rng: &mut StdRng, module_id: ModuleId) -> TransactionPayload {
//     GEN_ENTRY_POINTS
//         .choose(rng)
//         .unwrap()
//         .create_payload(module_id, Some(rng), None)
//}

//
// Entry points payload
//

fn loopy(module_id: ModuleId, count: u64) -> TransactionPayload {
    get_payload(module_id, ident_str!("loopy").to_owned(), vec![
        bcs::to_bytes(&count).unwrap(),
    ])
}

fn get_from_random_const(module_id: ModuleId, idx: u64) -> TransactionPayload {
    get_payload(
        module_id,
        ident_str!("get_from_random_const").to_owned(),
        vec![bcs::to_bytes(&idx).unwrap()],
    )
}

fn set_id(rng: &mut StdRng, module_id: ModuleId) -> TransactionPayload {
    let id: u64 = rng.gen();
    get_payload(module_id, ident_str!("set_id").to_owned(), vec![
        bcs::to_bytes(&id).unwrap(),
    ])
}

fn set_name(rng: &mut StdRng, module_id: ModuleId) -> TransactionPayload {
    let len = rng.gen_range(0usize, 1000usize);
    let name: String = rng
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect();
    get_payload(module_id, ident_str!("set_name").to_owned(), vec![
        bcs::to_bytes(&name).unwrap(),
    ])
}

fn maximize(module_id: ModuleId, other: AccountAddress) -> TransactionPayload {
    get_payload(module_id, ident_str!("maximize").to_owned(), vec![
        bcs::to_bytes(&other).unwrap(),
    ])
}

fn minimize(module_id: ModuleId, other: AccountAddress) -> TransactionPayload {
    get_payload(module_id, ident_str!("minimize").to_owned(), vec![
        bcs::to_bytes(&other).unwrap(),
    ])
}

fn make_or_change(
    rng: &mut StdRng,
    module_id: ModuleId,
    str_len: usize,
    data_len: usize,
) -> TransactionPayload {
    let id: u64 = rng.gen();
    let name: String = rng
        .sample_iter(&Alphanumeric)
        .take(str_len)
        .map(char::from)
        .collect();
    let mut bytes = Vec::<u8>::with_capacity(data_len);
    rng.fill_bytes(&mut bytes);
    get_payload(module_id, ident_str!("make_or_change").to_owned(), vec![
        bcs::to_bytes(&id).unwrap(),
        bcs::to_bytes(&name).unwrap(),
        bcs::to_bytes(&bytes).unwrap(),
    ])
}

fn bytes_make_or_change(
    rng: &mut StdRng,
    module_id: ModuleId,
    data_len: usize,
) -> TransactionPayload {
    let mut bytes = Vec::<u8>::with_capacity(data_len);
    rng.fill_bytes(&mut bytes);
    get_payload(
        module_id,
        ident_str!("bytes_make_or_change").to_owned(),
        vec![bcs::to_bytes(&bytes).unwrap()],
    )
}

fn get_payload_void(module_id: ModuleId, func: Identifier) -> TransactionPayload {
    get_payload(module_id, func, vec![])
}

fn get_payload(module_id: ModuleId, func: Identifier, args: Vec<Vec<u8>>) -> TransactionPayload {
    TransactionPayload::EntryFunction(EntryFunction::new(module_id, func, vec![], args))
}
