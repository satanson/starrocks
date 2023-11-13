// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.automv.util;

import com.google.common.base.Preconditions;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

public class Util {
    private static final String DATE_T_CLOCK_FMT = "yyyyMMdd'T'HHmmss";
    private static final String DIGEST_SHA1 = "SHA-1";
    private static final String DIGEST_MD5 = "MD5";
    private static final ThreadLocal<MessageDigest> TLS_SHA1_ALGO = new ThreadLocal<>();
    private static final ThreadLocal<MessageDigest> TLS_MD5_ALGO = new ThreadLocal<>();

    public static Supplier<Integer> nextIdGenerator(int from) {
        int[] id = new int[] {from};
        return () -> id[0]++;
    }

    public static Supplier<Integer> nextIdGenerator() {
        return nextIdGenerator(0);
    }

    public static Supplier<String> nextStringGenerator(String prefix, String suffix) {
        Supplier<Integer> nextId = nextIdGenerator();
        return () -> prefix + nextId.get() + suffix;
    }

    public static String toHexString(byte[] bytes) {
        StringBuilder s = new StringBuilder(bytes.length * 2);
        char[] d = "0123456789abcdef".toCharArray();
        for (byte a : bytes) {
            s.append(d[(a >>> 4) & 0xf]);
            s.append(d[a & 0xf]);
        }
        return s.toString();
    }

    public static boolean isHexString(String s) {
        if (s.length() % 2 != 0) {
            return false;
        }
        for (char ch : s.toCharArray()) {
            if (!('0' <= ch && ch <= '9' || 'a' <= ch && ch <= 'f')) {
                return false;
            }
        }
        return true;
    }

    public static String yyyyMMddTHHmmss() {
        return yyyyMMddTHHmmss(new Date());
    }

    public static String yyyyMMddTHHmmss(Date date) {
        DateFormat df = new SimpleDateFormat(DATE_T_CLOCK_FMT);
        return df.format(date);
    }

    public static Optional<Date> yyyyMMddTHHmmssToDate(String s) {
        DateFormat df = new SimpleDateFormat(DATE_T_CLOCK_FMT);
        try {
            return Optional.of(df.parse(s));
        } catch (ParseException e) {
            return Optional.empty();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T, S extends T> Optional<S> downcast(T obj, Class<S> klass) {
        Preconditions.checkArgument(obj != null);
        if (obj.getClass().equals(Objects.requireNonNull(klass))) {
            return Optional.of((S) obj);
        } else {
            return Optional.empty();
        }
    }

    private static MessageDigest getDigestAlgo(String algoName, ThreadLocal<MessageDigest> tlsAlgo) {
        if (tlsAlgo.get() != null) {
            return tlsAlgo.get();
        }
        try {
            MessageDigest digestAlgo = MessageDigest.getInstance(algoName);
            tlsAlgo.set(digestAlgo);
            return digestAlgo;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String sha1(String s) {
        MessageDigest algo = getDigestAlgo(DIGEST_SHA1, TLS_SHA1_ALGO);
        algo.update(s.getBytes(StandardCharsets.UTF_8));
        return Util.toHexString(algo.digest());
    }

    public static String md5(String s) {
        MessageDigest algo = getDigestAlgo(DIGEST_MD5, TLS_MD5_ALGO);
        algo.update(s.getBytes(StandardCharsets.UTF_8));
        return Util.toHexString(algo.digest());
    }
}
