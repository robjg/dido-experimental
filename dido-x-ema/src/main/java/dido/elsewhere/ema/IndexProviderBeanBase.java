package dido.elsewhere.ema;

import java.util.function.Function;

public class IndexProviderBeanBase {

    private int firstIndex;

    private int symbolsPerRecord;

    private Function<? super Integer, ? extends String> iToCain;

    private String prevLrName;

    private String nextLrName;

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

    public Function<? super Integer, ? extends String> getiToCain() {
        return iToCain;
    }

    public void setiToCain(Function<? super Integer, ? extends String> iToCain) {
        this.iToCain = iToCain;
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
}
