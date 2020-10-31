package client;

import java.security.MessageDigest;

import java.security.NoSuchAlgorithmException;

import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;

import javax.crypto.spec.SecretKeySpec;

import javax.xml.bind.DatatypeConverter;


public class Class1 {

    public String encrypt(String token, String clientkey) {
        System.out.println("TOken:"+token+" ClientKey:"+clientkey);
        Cipher ecipher;
        byte keyValue[]= null;
        try {
            ecipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            SecretKeySpec skey =
                new SecretKeySpec(Arrays.copyOf(clientkey.getBytes("UTF8"), 16),
                                  "AES");
            ecipher.init(Cipher.ENCRYPT_MODE, skey);
            keyValue = getSHA1(ecipher.doFinal(token.getBytes("UTF8")));
            
            
        } 
        catch (Exception e)
        {
        } 
        return new sun.misc.BASE64Encoder().encode(keyValue);
    }

    private static byte[] getSHA1(byte[] input) {
        MessageDigest sha1=null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            System.out.println("error1");
        }
        return sha1.digest(input);
    }

    public static void main(String[] args) {
        try {
            final String strPassPhrase =
                "8602816173982704148603252622819"; //min 24 chars --token

            String param = "K0R4YMU2S9R1B9YX"; // secret code
            Class1 c = new Class1();
            System.out.println("Text : " + c.encrypt(strPassPhrase,param));

            /*
            SecretKeyFactory factory = SecretKeyFactory.getInstance("DESede");
            SecretKey key =
                factory.generateSecret(new DESedeKeySpec(strPassPhrase.getBytes()));
            Cipher cipher = Cipher.getInstance("DESede");

            cipher.init(Cipher.ENCRYPT_MODE, key);
            String str =
                DatatypeConverter.printBase64Binary(cipher.doFinal(param.getBytes()));
            System.out.println("Text Encryted : " + str);

            cipher.init(Cipher.DECRYPT_MODE, key);
            String str2 =
                new String(cipher.doFinal(DatatypeConverter.parseBase64Binary(str)));
            System.out.println("Text Decryted : " + str2);*/

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
