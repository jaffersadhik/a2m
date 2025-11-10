package com.itextos.beacon.commonlib.pwdencryption;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

class BcryptHashing
{
    private static final int DEFAULT_HASH_ROUNDS = 10;
    private static final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder(DEFAULT_HASH_ROUNDS);

    private BcryptHashing()
    {}

    static EncryptedObject hash(String aPassword)
    {
        return hash(aPassword, DEFAULT_HASH_ROUNDS);
    }

    static EncryptedObject hash(String aStringToHash, int aDefaultHashRounds)
    {
        BCryptPasswordEncoder customEncoder = new BCryptPasswordEncoder(aDefaultHashRounds);
        final String hashed = customEncoder.encode(aStringToHash);
        return new EncryptedObject(aStringToHash, hashed);
    }

    static boolean isValidHash(String aUserPassword, String aDbHashValue)
    {
        return encoder.matches(aUserPassword, aDbHashValue);
    }
}