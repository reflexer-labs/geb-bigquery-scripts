SELECT DISTINCT address FROM (
    (
        # Query EOA that interacted with a specific list of contracts
        SELECT tx.from_address as address 
        # Table including all Ethereum transactions
        FROM bigquery-public-data.crypto_ethereum.transactions AS tx
        # Date just before PRAI deployement 
        WHERE tx.block_timestamp > TIMESTAMP("2020-10-24 00:00:00+00")
        # PRAI settlement block
        AND tx.block_number <= 11724918
        # The transaction had to succeed 
        AND tx.receipt_status = 1
        # Need to have interacted with at least one of these contracts
        AND (
            # Proxy factory address
            tx.to_address = "0xf89a0e02af0fd840b0fcf5d103e1b1c74c8b7638" 
            # PRAI token address
            OR tx.to_address = "0x715c3830fb0c4bab9a8e31c922626e1757716f3a"
            # PRAI uniswap pool
            OR tx.to_address = "0xebde9f61e34b7ac5aae5a4170e964ea85988008c"
            # Safe engine (to include unmanged safes)
            OR tx.to_address = "0xf0b7808b940b78be81ad6f9e075ce8be4a837e2c"
        )
    )

    UNION ALL

    (
        # Query to get all non-contract addresses that ever help PRAI
        SELECT to_address as address
        FROM bigquery-public-data.crypto_ethereum.token_transfers AS tx
        # Join contract table to filter out addresses that are contracts
        LEFT JOIN bigquery-public-data.crypto_ethereum.contracts as ctx ON tx.to_address = ctx.address 
        # Exclude all addresses that are a contract 
        WHERE ctx.address IS NULL 
        # Date just before PRAI deployement 
        AND tx.block_timestamp > TIMESTAMP("2020-10-24 00:00:00+00")
        # PRAI settlement block
        AND tx.block_number <= 11724918
        # PRAI token address
        AND token_address = "0x715c3830fb0c4bab9a8e31c922626e1757716f3a"
    )
)

WHERE address NOT IN UNNEST ([
    # List of addresses to exclude from the list, lower case only
    "0x0000000000000000000000000000000000000000", # Null address
    "0x0ce1ff652be78322e312e5073cd96b5e1cf5306e", # Bert multisig signer
    "0x3e0139ce3533a42a7d342841aee69ab2bfee1d51", # Fabio multisig signer
    "0x45c9a201e2937608905fef17de9a67f25f9f98e0", # Sean
    "0xbd3f90047b14e4f392d6877276d52d0ac59f4cf8", # Guillaume multisig signer
    "0x935a301ba674816524ceb4b1eabddb96c57ab805", # Stefan
    "0x6779122d59efdd6ec048fd5de02c2904ccffa259", # PRAI median pinger
    "0xa5ccb4286355b3412f1487aa52f5db93307aeaf7", # ETH median pinger
    "0xdf8f5cf7a2959f62009c655c896d3c0c6364d7d6", # Tax collector pinger
    "0x99fb4386310756522e727388bf5b68ccfaa22247", # FSM ETH pinger
    "0x6048cd849a6a1364a54a09f7cf430724695bbd0c", # RAI FSM pinger
    "0xa7691fc42dcba2efecd73675f90f119fcf1b6373", # Pause executor pinger
    "0xe8d944108afce391cdb7a0d90257e854c07fd918", # Stability fee treasury pinger
    "0x2b6216d0b1734cb73fdfd4bde616b761a3bddccf", # Debt settler pinger
    "0x4d1fb7a1aa8df65c169e76788baf4b68a72fca96", # Keeper?
    "0xdd1693bd8e307ecfdbe51d246562fc4109f871f8", # Bert arb/auctionKeeper
    "0xa346a2ed29750e8399a787946fabe06e81a39f3b", # Bert Reflexer SAFE
    "0x60efac991ae39fa6a594af58fd6fcb57940c3aa7", # Bert Reflexer SAFE
    "0x02b70c78b400ff8fe89af7d84d443f875d047a8f", # Bert Reflexer SAFE
    "0x871e1e0b7cdbc56ed8b682641158238562ca9ee4", # Bert Reflexer SAFE
    "0x953d1613063e9f3a5fb5cba849166d4d12992ccd", # Bert Reflexer SAFE
    "0xb9f4879d53259bde15a92b78d0da1c9f29767332", # Bert
    "0x25f952c6b87d3a9c48ac86b61f27b81a6f2ed332", # Bert
    "0x08717dc665247452454b6976a0fc6aab3a97d31f", # Guillaume Personal wallet
    "0xb193044b986956791cab713ff3cf9c1c474f2247", # Ameen safe account 5
    "0xf685e3819ad71772b4715425ba40e477b1d5d6bd", # Ameen safe account 4
    "0x4bea44985095bb98deef727ecc3509c9edfb1b19", # Ameen safe account 3
    "0x1d28a17529216cf013f62ecb889b874d65c9a55c", # Ameen safe account 2
    "0x9a52b4b63b1a4c9b25268254fac82001d2e687e1", # Ameen safe account 1
    "0x297bf847dcb01f3e870515628b36eabad491e5e8", # Ameen multisig signer
    "0x0cb27e883e207905ad2a94f9b6ef0c7a99223c37", # Sean personal wallet
    "0xfa5e4955a11902f849ecaddef355db69c2036de6", # Stefan multisig signer
    "0xe189bbb764ff0614cc608e09a49cc7569ff94521", # Stefan, another Reflexer account
    "0x94eed5e9ec2b85dbfa018d6978ee7e4126ad5134" # Stefan, another Reflexer account
])
ORDER BY address