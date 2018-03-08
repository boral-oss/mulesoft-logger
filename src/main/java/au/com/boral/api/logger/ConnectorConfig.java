package au.com.boral.api.logger;

import org.mule.api.annotations.components.Configuration;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.param.Default;

@Configuration(friendlyName = "Configuration")
public class ConnectorConfig {

    /**
     * Greeting message
     */
    @Configurable
    @Default("Hello")
    private String content;

    /**
     * Reply message
     */
    @Configurable
    @Default("Boral")
    private String reply;

    /**
     * Set greeting message
     *
     * @param greeting the greeting message
     */
    public void setGreeting(String greeting) {
        this.content = greeting;
    }

    /**
     * Get content message
     */
    public String getContent() {
        return this.content;
    }

    /**
     * Set reply
     *
     * @param reply the reply
     */
    public void setReply(String reply) {
        this.reply = reply;
    }

    /**
     * Get reply
     */
    public String getReply() {
        return this.reply;
    }

}