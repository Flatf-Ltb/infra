package io.netty.study.slidingwindow.pojo;

import io.flatf.foundation.serialization.json.JsonWriter;

/**
 * @author pancm
 * @version : 1.0.0
 * @title : Message
 * @description :
 * @date 2019年1月16日
 */
public class Message {

    private int id;
    private String cmd;
    private String msg;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCmd() {
        return cmd;
    }

    public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return JsonWriter.toJson(this);
    }

}
