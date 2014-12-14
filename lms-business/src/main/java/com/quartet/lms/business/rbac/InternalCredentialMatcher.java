package com.quartet.lms.business.rbac;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SaltedAuthenticationInfo;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.codec.Base64;
import org.apache.shiro.codec.CodecSupport;
import org.apache.shiro.crypto.hash.Md5Hash;
import org.apache.shiro.util.ByteSource;

/**
 * @author lcheng
 * @version 1.0
 *          ${tags}
 */
public class InternalCredentialMatcher extends CodecSupport implements CredentialsMatcher {

    @Override
    public boolean doCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) {
        Object tokenCredential = token.getCredentials();
        Object accountCredential = info.getCredentials();
        if (info instanceof SaltedAuthenticationInfo) {
            ByteSource salt = ((SaltedAuthenticationInfo) info).getCredentialsSalt();
            String encrypt = new String(salt.getBytes());
            String md51 = new Md5Hash(tokenCredential).toString();
            String s2 = md51 + encrypt;
            String md52 = new Md5Hash(s2).toString();
            return md52.equals(accountCredential);
        }
        return false;
    }
}
