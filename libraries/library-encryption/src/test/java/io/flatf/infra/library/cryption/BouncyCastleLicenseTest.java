package io.flatf.infra.library.cryption;

import org.bouncycastle.LICENSE;
import org.slf4j.Logger;

import javax.annotation.Nullable;

public class BouncyCastleLicenseTest {

	/**
	 * 
	 */
	public static final void showLicense() {
		showLicense(null);
	}

	/**
	 * 
	 * @param log
	 */
	public static final void showLicense(@Nullable Logger log) {
		if (log != null)
			log.info(LICENSE.licenseText);
		else
			System.out.println(LICENSE.licenseText);
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(LICENSE.licenseText);
	}

}

