package dido.vertx.util;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class SectionTextOutTest {


    @Test
    void nameNested() {

        StringBuilder sb = new StringBuilder();

        SectionTextOut.of(sb)
                .nested("Meals", meals -> meals
                        .nested("Breakfast", breakfast -> breakfast
                                .nested("Fruit", fruit -> {
                                }))
                        .nested("Lunch", lunch -> lunch
                                .value("Soup", "Tomato")));

        assertThat(sb.toString(), is(
                """
                        ---- Meals ----
                          ---- Breakfast ----
                            ---- Fruit ----
                          ---- Lunch ----
                            Soup: Tomato
                        """));
    }

    @Test
    void nestedValues() {

        StringBuilder sb = new StringBuilder();

        SectionTextOut.of(sb)
                .nested("Fruit", out -> out
                        .value("Apple", "Red")
                        .value("Banana", "Yellow"));

        assertThat(sb.toString(), is(
                """
                        ---- Fruit ----
                          Apple: Red
                          Banana: Yellow
                        """));
    }

    @Test
    void nestedRepeating() {

        StringBuilder sb = new StringBuilder();

        SectionTextOut.of(sb)
                .repeating("Meals", rep -> rep
                        .item(item -> item
                                .nested("Breakfast", breakfast -> breakfast
                                        .repeating("Fruits", fruits -> fruits
                                                .item(fruit -> fruit.value("Apple", "Red"))
                                                .item(fruit -> fruit.value("Banana", "Yellow"))
                                        )
                                        .value("drink", "OJ")))
                        .item(item -> item
                                .nested("Lunch", lunch -> lunch
                                        .value("Soup", "Tomato")
                                        .value("Drink", "Water"))));

        assertThat(sb.toString(), is(
                """
                        ---- Meals -----
                          - ---- Breakfast ----
                            - ---- Fruits -----
                              - Apple: Red
                              - Banana: Yellow
                            - drink: OJ
                          - ---- Lunch ----
                            - Soup: Tomato
                            - Drink: Water
                        """));
    }

}