package com.itextos.beacon.commonlib.pwdencryption;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.security.spec.KeySpec;
import java.security.NoSuchAlgorithmException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import com.itextos.beacon.commonlib.constants.Constants;
import com.itextos.beacon.commonlib.constants.exception.ItextosRuntimeException;

public final class Encryptor {
    private static final int KDF_ITERATIONS = 210_000; // OWASP recommended for PBKDF2
    private static final int KDF_KEY_LENGTH = 256; // bits
    private static final String KDF_ALGORITHM = "PBKDF2WithHmacSHA256";
    
    // Private constructor to prevent instantiation
    private Encryptor() {
        throw new AssertionError("Cannot instantiate utility class");
    }

    public static EncryptedObject encrypt(CryptoType cryptoType, String toEncrypt, String key) throws Exception {
        return switch (cryptoType) {
            case ENCRYPTION_AES_256 -> Aes256Encrypt.encrypt(toEncrypt, key);
            case HASHING_BCRYPT -> BcryptHashing.hash(toEncrypt);
            case EMPTY -> new EncryptedObject(toEncrypt, "");
            case ENCODE -> new EncryptedObject(toEncrypt, 
                Base64.getEncoder().encodeToString(toEncrypt.getBytes(StandardCharsets.UTF_8)));
            default -> throw new ItextosRuntimeException(
                "Invalid crypto type specified. CryptoType: '%s'".formatted(cryptoType));
        };
    }

    public static String decrypt(CryptoType cryptoType, String toDecrypt, String key) throws Exception {
        return switch (cryptoType) {
            case ENCRYPTION_AES_256 -> Aes256Encrypt.decrypt(toDecrypt, key);
            case ENCODE -> new String(Base64.getDecoder().decode(toDecrypt), StandardCharsets.UTF_8);
            case HASHING_BCRYPT, EMPTY -> 
                throw new ItextosRuntimeException("Not applicable for decrypt. CryptoType: '%s'".formatted(cryptoType));
            default -> throw new ItextosRuntimeException(
                "Invalid crypto type specified. CryptoType: '%s'".formatted(cryptoType));
        };
    }

    // Modern KDF implementation using PBKDF2
    public static byte[] deriveKey(String password, byte[] salt) throws ItextosRuntimeException {
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance(KDF_ALGORITHM);
            KeySpec spec = new PBEKeySpec(
                password.toCharArray(), 
                salt, 
                KDF_ITERATIONS, 
                KDF_KEY_LENGTH
            );
            return factory.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException e) {
            throw new ItextosRuntimeException("KDF algorithm not available: " + KDF_ALGORITHM, e);
        } catch (Exception e) {
            throw new ItextosRuntimeException("Key derivation failed", e);
        }
    }

    // Enhanced base64 decoding with better error handling
    private static String base64Decode(String encoded) throws ItextosRuntimeException {
        if (encoded == null || encoded.isBlank()) {
            return encoded;
        }
        
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(encoded);
            return new String(decodedBytes, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new ItextosRuntimeException("Invalid Base64 input: " + encoded, e);
        }
    }

    // Factory methods using sealed interfaces (if you can modify CryptoType)
    public static EncryptedObject getEncryptedDbPassword(String dbPassword) throws Exception {
        return encrypt(CryptoType.ENCRYPTION_AES_256, dbPassword, PasswordConstants.DB_PASSWORD_KEY);
    }

    public static String getDecryptedDbPassword(String encryptedDbPassword) throws Exception {
        return decrypt(CryptoType.ENCRYPTION_AES_256, encryptedDbPassword, PasswordConstants.DB_PASSWORD_KEY);
    }

    public static EncryptedObject getApiPassword() throws Exception {
        return encrypt(CryptoType.ENCRYPTION_AES_256, RandomString.getApiPassword(), PasswordConstants.API_PASSWORD_KEY);
    }

    public static String getApiDecryptedPassword(String dbPassword) throws Exception {
        return decrypt(CryptoType.ENCRYPTION_AES_256, dbPassword, PasswordConstants.API_PASSWORD_KEY);
    }

    public static EncryptedObject getSmppPassword() throws Exception {
        return encrypt(CryptoType.ENCRYPTION_AES_256, RandomString.getSmppPassword(), PasswordConstants.SMPP_PASSWORD_KEY);
    }

    public static String getSmppDecryptedPassword(String dbPassword) throws Exception {
        return decrypt(CryptoType.ENCRYPTION_AES_256, dbPassword, PasswordConstants.SMPP_PASSWORD_KEY);
    }

    public static EncryptedObject getGuiPassword() throws Exception {
        return encrypt(CryptoType.HASHING_BCRYPT, RandomString.getGuiPassword(), null);
    }
    
    public static EncryptedObject getGuiPassword(String password) throws Exception {
        return encrypt(CryptoType.HASHING_BCRYPT, password, null);
    }
    
    // This method name is misleading - it's decoding, not decrypting
    public static String getGuiDecodedPassword(String pass) throws Exception {
        return decrypt(CryptoType.ENCODE, pass, null);
    }

    // Main method with better structure
    public static void main(String[] args) {
        if (args.length > 0) {
            handleCommandLine(args);
            return;
        }
        
        runTests();
    }

    private static void handleCommandLine(String[] args) {
        // Your command line handling logic here
    }

    private static void runTests() {
        try {
            long start = System.currentTimeMillis();
            
            // Performance and functionality tests
            String apiPassword = getApiDecryptedPassword("46AsqfI5S3lclHDF4GbXe0YyZkp5SVhWdWeN7Iin9T5APveHY+CtB1ir");
            long end = System.currentTimeMillis();
            
            System.out.println("Time Taken: " + (end - start) + "ms");
            
            // Additional test cases
            String dbPassword = getDecryptedDbPassword("N5mIleJjtYx2EFg8+cd3uFpGaUgxdEpKQjde+JBw9AjmsAX7iQEVAvlI");
            
            String encodedString = URLEncoder.encode("a", StandardCharsets.UTF_8);
            System.out.println(encodedString);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}