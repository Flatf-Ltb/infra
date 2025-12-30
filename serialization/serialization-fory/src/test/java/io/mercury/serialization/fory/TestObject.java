package io.mercury.serialization.fory;

/**
 * Test
 */
public class TestObject {

    private long ordSysId;

    private String instrumentCode;

    private int accountId;

    private String remark;

    public long getOrdSysId() {
        return ordSysId;
    }

    public TestObject setOrdSysId(long ordSysId) {
        this.ordSysId = ordSysId;
        return this;
    }

    public String getInstrumentCode() {
        return instrumentCode;
    }

    public TestObject setInstrumentCode(String instrumentCode) {
        this.instrumentCode = instrumentCode;
        return this;
    }

    public int getAccountId() {
        return accountId;
    }

    public TestObject setAccountId(int accountId) {
        this.accountId = accountId;
        return this;
    }

    public String getRemark() {
        return remark;
    }

    public TestObject setRemark(String remark) {
        this.remark = remark;
        return this;
    }

}
