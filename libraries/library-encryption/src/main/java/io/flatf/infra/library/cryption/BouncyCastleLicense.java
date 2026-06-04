package io.flatf.infra.library.cryption;

import org.bouncycastle.LICENSE;
import org.slf4j.Logger;

import javax.annotation.Nullable;

public class BouncyCastleLicense {

    public static void showLicense() {
        showLicense(null);
    }

    /**
     * @param log Logger
     */
    public static void showLicense(@Nullable Logger log) {
        if (log != null)
            log.info(LICENSE.licenseText);
        else
            System.out.println(LICENSE.licenseText);
    }

    public static void main(String[] args) {
        System.out.println(LICENSE.licenseText);
    }

}
