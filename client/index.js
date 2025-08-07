
import fetch from 'node-fetch';

const BACKEND_URL = 'http://localhost:5001';

export async function xadd(stream, fields) {
    const res = await fetch(`${BACKEND_URL}/xadd/${stream}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fields)
    });
    if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
    }
    return res.json();
}

export async function xrange(stream, count = 10) {
    const res = await fetch(`${BACKEND_URL}/xrange/${stream}?count=${count}`);
    if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
    }
    return res.json();
}

export async function xlen(stream) {
    const res = await fetch(`${BACKEND_URL}/xlen/${stream}`);
    if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
    }
    return res.json();
}

export async function xread(streams, ids, count = 10) {
    const params = new URLSearchParams();
    params.append('streams', streams.map((stream, index) => `${stream} ${ids[index]}`).join(' '));
    params.append('count', count);
    const res = await fetch(`${BACKEND_URL}/xread?${params.toString()}`);
    if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
    }
    return res.json();
}
