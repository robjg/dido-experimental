package dido.elsewhere.ema;

public class IndexProviderBeanBase {

    private int firstIndex;

    private int symbolsPerRecord;

    private String chainNameFormat;

    private Integer firstLink;

    private String prevLrName;

    private String nextLrName;

    private String refCountName;

    private String linkFieldFormat;

    public int getFirstIndex() {
        return firstIndex;
    }

    public void setFirstIndex(int firstIndex) {
        this.firstIndex = firstIndex;
    }

    public int getSymbolsPerRecord() {
        return symbolsPerRecord;
    }

    public void setSymbolsPerRecord(int symbolsPerRecord) {
        this.symbolsPerRecord = symbolsPerRecord;
    }

    public String getChainNameFormat() {
        return chainNameFormat;
    }

    public void setChainNameFormat(String chainNameFormat) {
        this.chainNameFormat = chainNameFormat;
    }

    public Integer getFirstLink() {
        return firstLink;
    }

    public void setFirstLink(Integer firstLink) {
        this.firstLink = firstLink;
    }

    public String getPrevLrName() {
        return prevLrName;
    }

    public void setPrevLrName(String prevLrName) {
        this.prevLrName = prevLrName;
    }

    public String getNextLrName() {
        return nextLrName;
    }

    public void setNextLrName(String nextLrName) {
        this.nextLrName = nextLrName;
    }

    public String getLinkFieldFormat() {
        return linkFieldFormat;
    }

    public String getRefCountName() {
        return refCountName;
    }

    public void setRefCountName(String refCountName) {
        this.refCountName = refCountName;
    }

    public void setLinkFieldFormat(String linkFieldFormat) {
        this.linkFieldFormat = linkFieldFormat;
    }
}
