package org.mskcc.cmo.messaging;

import java.io.IOException;

public interface FileUtil {
    public void savePublishFailureMessage(String topic, String message) throws IOException;
}
