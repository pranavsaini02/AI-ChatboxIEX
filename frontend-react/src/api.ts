export async function chatApi(prompt: string, extras: any = {}): Promise<any> {
  const res = await fetch('http://127.0.0.1:8000/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prompt, ...extras })
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}
