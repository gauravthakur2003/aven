// M2g — Vision Colour Detection
// Uses Groq llama-3.2-11b-vision-preview to detect exterior car colour from listing images.
// Only called when colour_exterior is null and listing images exist.

import OpenAI from 'openai';

const GROQ_BASE_URL = 'https://api.groq.com/openai/v1';
const VISION_MODEL  = 'llama-3.2-11b-vision-preview';
const TIMEOUT_MS    = 30_000;

let _client: OpenAI | null = null;
function getClient(): OpenAI {
  if (!_client) _client = new OpenAI({ apiKey: process.env.GROQ_API_KEY ?? '', baseURL: GROQ_BASE_URL, timeout: TIMEOUT_MS });
  return _client;
}

export interface VisionResult {
  colour: string | null;
  source: 'vision';
}

const PROMPT = [
  'Look at the car in this image.',
  'What is the exterior colour of the car?',
  'Reply with ONLY the colour name, nothing else.',
  'Use one of: White, Black, Silver, Grey, Red, Blue, Navy, Green, Brown, Beige, Gold, Orange, Yellow, Purple, Burgundy, Bronze.',
  'If no car is clearly visible, reply: Unknown.',
].join(' ');

export async function detectColour(imageUrls: string[]): Promise<VisionResult> {
  if (!imageUrls.length) return { colour: null, source: 'vision' };
  if (!process.env.GROQ_API_KEY) return { colour: null, source: 'vision' };

  // Try first two images — sometimes the first is an interior shot
  for (const url of imageUrls.slice(0, 2)) {
    try {
      const response = await getClient().chat.completions.create({
        model: VISION_MODEL, max_tokens: 20, temperature: 0,
        messages: [{
          role: 'user',
          content: [
            { type: 'image_url', image_url: { url } },
            { type: 'text', text: PROMPT },
          ],
        }],
      });
      const raw = (response.choices[0]?.message?.content ?? '').trim();
      if (raw && raw.toLowerCase() !== 'unknown' && raw.length <= 20) {
        // Capitalise first letter
        return { colour: raw.charAt(0).toUpperCase() + raw.slice(1), source: 'vision' };
      }
    } catch {
      // Try next image on error
    }
  }

  return { colour: null, source: 'vision' };
}
