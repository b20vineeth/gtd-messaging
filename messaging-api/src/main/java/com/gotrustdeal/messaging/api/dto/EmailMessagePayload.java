package com.gotrustdeal.messaging.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EmailMessagePayload {
    private String channel;
    private String templateCode;
    private Recipient recipient;
    private Map<String, String> variables;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Recipient {
        private String email;
    }
}
