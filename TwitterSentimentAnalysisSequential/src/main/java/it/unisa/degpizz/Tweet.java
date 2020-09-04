package it.unisa.degpizz;

public class Tweet {
    private String text;
    private String lang;

    public Tweet() {
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public Tweet(String text, String lang) {

        this.text = text;
        this.lang = lang;
    }
}
