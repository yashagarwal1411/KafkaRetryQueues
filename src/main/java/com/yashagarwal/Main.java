package com.yashagarwal;

import com.yashagarwal.helpers.EmailHelperWithRetry;
import com.yashagarwal.messaging.MessageProcessorWithRetry;

public class Main {

    public static void main(String[] args) {

        /* Initialise application scoped */
        new MessageProcessorWithRetry();
        new EmailHelperWithRetry();

    }
}
