import WebSocket from "ws";

export function registerInboundRoutes(fastify) {
  // Check for required environment variables
  const { ELEVENLABS_API_KEY, ELEVENLABS_AGENT_ID } = process.env;
  if (!ELEVENLABS_API_KEY || !ELEVENLABS_AGENT_ID) {
    console.error("Missing required environment variables");
    throw new Error("Missing ELEVENLABS_API_KEY or ELEVENLABS_AGENT_ID");
  }

  // Helper function to get signed URL for authenticated conversations
  async function getSignedUrl() {
    try {
      const response = await fetch(
        `https://api.elevenlabs.io/v1/convai/conversation/get_signed_url?agent_id=${ELEVENLABS_AGENT_ID}`,
        {
          method: "GET",
          headers: {
            "xi-api-key": ELEVENLABS_API_KEY,
          },
        },
      );
      if (!response.ok) {
        throw new Error(`Failed to get signed URL: ${response.statusText}`);
      }
      const data = await response.json();
      return data.signed_url;
    } catch (error) {
      console.error("Error getting signed URL:", error);
      throw error;
    }
  }

  // Twilio inbound call TwiML route
  fastify.all("/incoming-call-eleven", async (request, reply) => {
    console.log("[DEBUG] Received incoming call request from Twilio.");
    const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
      <Response>
        <Connect>
          <Stream url="wss://${request.headers.host}/inbound-media-stream" />
        </Connect>
      </Response>`;
    reply.type("text/xml").send(twimlResponse);
  });

  // WebSocket route for handling media streams from Twilio (Inbound)
  fastify.register(async (fastifyInstance) => {
    fastifyInstance.get(
      "/inbound-media-stream",
      { websocket: true },
      (ws, req) => {
        console.info("[DEBUG] Twilio connected to inbound media stream.");

        let streamSid = null;
        let elevenLabsWs = null;

        // Handle Twilio WebSocket errors
        ws.on("error", (err) => console.error("[Twilio] WS error:", err));

        // Function to setup the ElevenLabs connection
        async function setupElevenLabs() {
          try {
            console.log("[DEBUG] Getting signed URL...");
            const signedUrl = await getSignedUrl();
            console.log("[DEBUG] Signed URL received:", signedUrl);

            elevenLabsWs = new WebSocket(signedUrl);
            console.log(
              "[DEBUG] Connecting to ElevenLabs with URL:",
              signedUrl,
            );

            elevenLabsWs.on("open", () => {
              console.log("[DEBUG] Connected to ElevenLabs Conversational AI.");

              // Send initial configuration (customize prompt and first message as needed)
              const initialConfig = {
                type: "conversation_initiation_client_data",
              };
              console.log(
                "[DEBUG] Sending initial configuration to ElevenLabs:",
                JSON.stringify(initialConfig, null, 2),
              );
              elevenLabsWs.send(JSON.stringify(initialConfig));
            });

            elevenLabsWs.on("message", (data) => {
              try {
                const message = JSON.parse(data);
                console.log(
                  `[DEBUG] Received message from ElevenLabs, type: ${message.type}`,
                );
                switch (message.type) {
                  case "conversation_initiation_metadata":
                    console.log(
                      "[DEBUG] Received initiation metadata from ElevenLabs.",
                    );
                    break;
                  case "audio":
                    if (streamSid) {
                      if (message.audio?.chunk) {
                        const audioData = {
                          event: "media",
                          streamSid,
                          media: {
                            payload: message.audio.chunk,
                          },
                        };
                        console.log(
                          "[DEBUG] Forwarding ElevenLabs audio chunk to Twilio.",
                        );
                        ws.send(JSON.stringify(audioData));
                      } else if (message.audio_event?.audio_base_64) {
                        const audioData = {
                          event: "media",
                          streamSid,
                          media: {
                            payload: message.audio_event.audio_base_64,
                          },
                        };
                        console.log(
                          "[DEBUG] Forwarding ElevenLabs audio event to Twilio.",
                        );
                        ws.send(JSON.stringify(audioData));
                      }
                    } else {
                      console.log(
                        "[DEBUG] Received audio but no streamSid yet.",
                      );
                    }
                    break;
                  case "interruption":
                    if (streamSid) {
                      ws.send(JSON.stringify({ event: "clear", streamSid }));
                    }
                    break;
                  case "ping":
                    if (message.ping_event?.event_id) {
                      const pong = {
                        type: "pong",
                        event_id: message.ping_event.event_id,
                      };
                      elevenLabsWs.send(JSON.stringify(pong));
                    }
                    break;
                  default:
                    console.log(
                      "[DEBUG] Unhandled message type from ElevenLabs:",
                      message.type,
                    );
                }
              } catch (err) {
                console.error(
                  "[DEBUG] Error processing message from ElevenLabs:",
                  err,
                );
              }
            });

            elevenLabsWs.on("error", (err) => {
              console.error("[DEBUG] ElevenLabs WebSocket error:", err);
            });

            elevenLabsWs.on("close", () => {
              console.log("[DEBUG] ElevenLabs WebSocket disconnected.");

              ws.close();
              console.log("[DEBUG] Closed Twilio connection due to ElevenLabs disconnect.");
              
            });
          } catch (error) {
            console.error(
              "[DEBUG] Error setting up ElevenLabs connection:",
              error,
            );
          }
        }

        // Initiate connection to ElevenLabs
        setupElevenLabs();

        // Handle messages from Twilio
        ws.on("message", (message) => {
          try {
            console.log("[DEBUG] Received message from Twilio:", message);
            const data = JSON.parse(message);
            switch (data.event) {
              case "start":
                streamSid = data.start.streamSid;
                console.log(
                  `[DEBUG] Stream started with StreamSid: ${streamSid}`,
                );
                break;
              case "media":
                if (
                  elevenLabsWs &&
                  elevenLabsWs.readyState === WebSocket.OPEN
                ) {
                  // Forward audio chunk to ElevenLabs
                  const audioMessage = {
                    user_audio_chunk: Buffer.from(
                      data.media.payload,
                      "base64",
                    ).toString("base64"),
                  };
                  console.log(
                    "[DEBUG] Forwarding audio chunk to ElevenLabs:",
                    audioMessage,
                  );
                  elevenLabsWs.send(JSON.stringify(audioMessage));
                } else {
                  console.log(
                    "[DEBUG] ElevenLabs WebSocket not open. Cannot forward audio chunk.",
                  );
                }
                break;
              case "stop":
                console.log(`[DEBUG] Stream ${streamSid} ended`);
                if (
                  elevenLabsWs &&
                  elevenLabsWs.readyState === WebSocket.OPEN
                ) {
                  elevenLabsWs.close();
                }
                break;
              default:
                console.log("[DEBUG] Unhandled event from Twilio:", data.event);
            }
          } catch (error) {
            console.error("[DEBUG] Error processing Twilio message:", error);
          }
        });

        // Handle WebSocket closure
        ws.on("close", () => {
          console.log("[DEBUG] Twilio client disconnected");
          if (elevenLabsWs && elevenLabsWs.readyState === WebSocket.OPEN) {
            elevenLabsWs.close();
          }
        });
      },
    );
  });
}
