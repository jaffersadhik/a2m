package com.itextos.beacon.commonlib.pwdencryption;

import java.time.Duration;

/**
 * Security configuration with environment-specific settings.
 */
public final class SecurityConfig {
    
    private SecurityConfig() {}
    
    // Key rotation intervals
    public static final Duration KEY_ROTATION_INTERVAL = Duration.ofDays(90);
    public static final Duration TOKEN_EXPIRY = Duration.ofHours(24);
    
    // Rate limiting
    public static final int MAX_AUTH_ATTEMPTS_PER_MINUTE = 10;
    public static final int MAX_ENCRYPTION_REQUESTS_PER_MINUTE = 1000;
    
    // Memory limits for password hashing (for bcrypt equivalent)
    public static final int MEMORY_COST = 65536; // 64MB
    public static final int PARALLELISM = 1;
    
    // Audit logging
    public static final boolean ENABLE_SECURITY_AUDIT = true;
    public static final Duration AUDIT_RETENTION = Duration.ofDays(365);
    
    // Compliance settings
    public static final boolean ENFORCE_COMPLEXITY = true;
    public static final boolean PREVENT_COMMON_PASSWORDS = true;
}