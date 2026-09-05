package com.orderflow.order.web;

import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

/**
 * Sends the root path to the demo page.
 *
 * <p>Without this, {@code GET /} returns a 404 in the API's error envelope —
 * correct, since the root is not an endpoint, but useless to someone who has
 * been handed the URL and typed it into a browser.
 *
 * <p>It points at the demo rather than Swagger deliberately. Swagger answers
 * "what endpoints exist"; the demo answers "what happens when payment fails
 * after stock is already reserved", which is the question this project exists
 * to answer and the harder one to convey in prose.
 *
 * <p>Hidden from the OpenAPI document: a convenience for people, not part of
 * the API contract.
 */
@RestController
@Hidden
public class RootController {

    @GetMapping("/")
    public RedirectView root() {
        return new RedirectView("/demo/index.html");
    }

    /**
     * Spring Boot resolves {@code index.html} for the application root only, so a
     * directory-style request one level down finds neither a handler nor a static
     * resource and has to be mapped explicitly.
     */
    @GetMapping("/demo/")
    public RedirectView demo() {
        return new RedirectView("/demo/index.html");
    }
}
