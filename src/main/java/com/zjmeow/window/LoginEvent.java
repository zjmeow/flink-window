package com.zjmeow.window;

import lombok.Data;

/**
 * 登录事件
 *
 * @author zjmeow
 */
@Data
public class LoginEvent extends BaseEvent {

    private String userId;
    private String ip;
}
