package com.zjmeow.window;

import lombok.Data;

/**
 * 带有事件和是否输出过标记的基础事件
 *
 * @author zjmeow
 */
@Data
public abstract class BaseEvent {

    private long time;
    private boolean outputted;
}
