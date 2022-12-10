package io.github.pulsarkeeper.cli;

import java.util.Arrays;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@ThreadSafe
public class Args {
    @Setter
    @Getter
    private boolean empty;
    @Setter
    @Getter
    private String rootCommand;
    @Setter
    private String[] args;
    private volatile int leftIndex = 0;

    private Args() {

    }

    public static Args wrap(@NonNull String[] args) {
        if (args.length < 1) {
            return empty();
        }
        Args argObj = new Args();
        argObj.setEmpty(false);
        argObj.setRootCommand(args[0]);
        argObj.setArgs(args);
        return argObj;
    }


    public static Args empty() {
        Args args = new Args();
        args.setRootCommand("");
        args.setArgs(new String[]{});
        args.setEmpty(true);
        return args;
    }

    public void shift() {
        shift(1);
    }

    public synchronized void shift(int position) {
        int updatedLeftIndex = leftIndex + position;
        if (updatedLeftIndex > args.length) {
            return;
        }
        leftIndex = updatedLeftIndex;
    }

    public String[] getArgs() {
        return Arrays.copyOfRange(args, leftIndex, args.length);
    }
}
