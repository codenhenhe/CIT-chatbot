'use client'

import { useState, useRef, useEffect } from "react"
import { 
  Send, Loader2, MessageSquare, Plus, User, Bot, 
  Trash2, Github, Copy, ThumbsUp, RefreshCw, Share2, Building2, X
} from "lucide-react"
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

type Message = {
  id: string
  role: "user" | "assistant"
  parts: { type: "text"; text: string }[]
}

type Conversation = {
  id: string
  title: string
  messages: Message[]
  createdAt: number
}

const STORAGE_KEY = "cictbot_conversations"

function loadConversations(): Conversation[] {
  if (typeof window === "undefined") return []
  const stored = localStorage.getItem(STORAGE_KEY)
  if (stored) {
    try {
      return JSON.parse(stored)
    } catch {
      return []
    }
  }
  return []
}

function saveConversations(conversations: Conversation[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(conversations))
}

export default function Chat() {
  const [conversations, setConversations] = useState<Conversation[]>([])
  const [activeConversationId, setActiveConversationId] = useState<string | null>(null)
  const [input, setInput] = useState("")
  const [status, setStatus] = useState<"idle" | "streaming">("idle")
  const [isSidebarOpen, setIsSidebarOpen] = useState(false)

  const activeConversation = conversations.find(c => c.id === activeConversationId)
  const messages = activeConversation?.messages || []

  const scrollRef = useRef<HTMLDivElement>(null)
  const bufferRef = useRef("")
  const flushTimer = useRef<NodeJS.Timeout | null>(null)

  // Load conversations from localStorage on mount
  useEffect(() => {
    const loaded = loadConversations()
    if (loaded.length > 0) {
      setConversations(loaded)
      setActiveConversationId(loaded[0].id)
    }
  }, [])

  // Save conversations to localStorage whenever they change
  useEffect(() => {
    if (conversations.length > 0) {
      saveConversations(conversations)
    }
  }, [conversations])

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTo({
        top: scrollRef.current.scrollHeight,
        behavior: "smooth"
      })
    }
  }, [messages])

  const flushBuffer = (assistantId: string) => {
    const text = bufferRef.current
    bufferRef.current = ""
    if (!text) return

    setConversations(prev => prev.map(c => {
      if (c.id !== activeConversationId) return c
      return {
        ...c,
        messages: c.messages.map(m =>
          m.id === assistantId
            ? { ...m, parts: [{ type: "text", text: m.parts[0].text + text }] }
            : m
        )
      }
    }))
  }

  const createNewConversation = () => {
    const conversationNumber = conversations.length + 1
    const newConv: Conversation = {
      id: Date.now().toString(),
      title: `Hội thoại ${conversationNumber}`,
      messages: [],
      createdAt: Date.now()
    }
    setConversations(prev => [newConv, ...prev])
    setActiveConversationId(newConv.id)
    setInput("")
  }

  const switchConversation = (id: string) => {
    setActiveConversationId(id)
    setIsSidebarOpen(false)
  }

  const deleteConversation = (id: string, e: React.MouseEvent) => {
    e.stopPropagation()
    const newConvs = conversations.filter(c => c.id !== id)
    setConversations(newConvs)
    if (activeConversationId === id) {
      setActiveConversationId(newConvs[0]?.id || null)
    }
    saveConversations(newConvs)
  }

  const handleSend = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || status === "streaming") return

    // Create new conversation if none exists
    let currentConvId = activeConversationId
    if (!currentConvId) {
      const conversationNumber = conversations.length + 1
      const newConv: Conversation = {
        id: Date.now().toString(),
        title: `Hội thoại ${conversationNumber}`,
        messages: [],
        createdAt: Date.now()
      }
      setConversations(prev => [newConv, ...prev])
      currentConvId = newConv.id
      setActiveConversationId(currentConvId)
    }

    const userMsg: Message = {
      id: Date.now().toString(),
      role: "user",
      parts: [{ type: "text", text: input }]
    }

    // Update conversation with user message
    setConversations(prev => prev.map(c => {
      if (c.id !== currentConvId) return c
      return {
        ...c,
        messages: [...c.messages, userMsg]
      }
    }))

    setInput("")
    setStatus("streaming")

    // Get current messages for API call
    const currentMessages = conversations.find(c => c.id === currentConvId)?.messages || []
    const newMessages = [...currentMessages, userMsg]

    try {
      const response = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: newMessages.map(m => ({ role: m.role, parts: m.parts }))
        })
      })

      if (!response.body) throw new Error("No body")

      const assistantId = (Date.now() + 1).toString()
      
      // Add assistant message placeholder
      setConversations(prev => prev.map(c => {
        if (c.id !== currentConvId) return c
        return {
          ...c,
          messages: [
            ...c.messages,
            { id: assistantId, role: "assistant", parts: [{ type: "text", text: "" }] }
          ]
        }
      }))

      const reader = response.body.getReader()
      const decoder = new TextDecoder()

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        const chunk = decoder.decode(value)
        const lines = chunk.split("\n")

        for (const line of lines) {
          if (!line.startsWith("0:")) continue
          try {
            const token = JSON.parse(line.slice(2))
            bufferRef.current += token
            if (!flushTimer.current) {
              flushTimer.current = setTimeout(() => {
                flushBuffer(assistantId)
                flushTimer.current = null
              }, 40)
            }
          } catch (err) { console.error(err) }
        }
      }
      flushBuffer(assistantId)
    } catch (err) {
      console.error("Chat error:", err)
    }
    setStatus("idle")
  }

  return (
    <div className="flex h-screen bg-[#F8FAFC] font-sans text-slate-900 selection:bg-blue-100">
      {/* Sidebar - Modern Glassmorphism */}
      <aside className={`
        fixed md:relative z-40 w-80 bg-white/70 backdrop-blur-xl border-r border-slate-200/60 flex flex-col
        h-full transition-transform duration-300 ease-in-out
        ${isSidebarOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0'}
      `}>
        {/* Top section - scrollable */}
        <div className="flex-1 overflow-y-auto">
          <div className="p-6 flex items-center gap-3">
            <div className="w-10 h-10 bg-linear-to-br from-blue-600 to-indigo-700 rounded-xl flex items-center justify-center text-white shadow-lg shadow-blue-200">
              <Bot size={24} />
            </div>
            <div>
              <h1 className="font-bold text-slate-800 tracking-tight leading-none">CICTBot</h1>
              <span className="text-[10px] text-blue-600 font-bold uppercase tracking-widest">Trợ lý ảo Văn phòng Trường CNTT-TT</span>
            </div>
            <button 
              onClick={() => setIsSidebarOpen(false)}
              className="md:hidden ml-auto p-2 text-slate-400 hover:text-slate-600 hover:bg-slate-100 rounded-lg"
            >
              <X size={20}/>
            </button>
          </div>

          <div className="p-4">
            <button 
              onClick={createNewConversation}
              className="w-full flex items-center justify-center gap-2 p-3 bg-white text-slate-700 rounded-xl hover:bg-slate-50 transition-all font-semibold border border-slate-200 shadow-sm active:scale-[0.98]"
            >
              <Plus size={18}/> Cuộc hội thoại mới
            </button>
          </div>

          <div className="px-4 py-2 space-y-1">
          <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider px-3 mt-4 mb-2">Hội thoại</div>
          {conversations.length === 0 ? (
            <div className="text-sm text-slate-400 px-3 py-4 text-center">
              Chưa có hội thoại nào
            </div>
          ) : (
            conversations.map(conv => (
              <div 
                key={conv.id}
                onClick={() => switchConversation(conv.id)}
                className={`group flex items-center gap-3 p-3 rounded-xl cursor-pointer transition-all text-sm ${
                  activeConversationId === conv.id
                    ? "bg-blue-50/50 border border-blue-100/50 text-blue-700 font-medium"
                    : "text-slate-600 hover:bg-slate-50 border border-transparent"
                }`}
              >
                <MessageSquare size={16} className={activeConversationId === conv.id ? "text-blue-500" : "text-slate-400"}/>
                <span className="truncate flex-1">{conv.title}</span>
                <button 
                  onClick={(e) => deleteConversation(conv.id, e)}
                  className="opacity-0 group-hover:opacity-100 p-1 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded transition-all"
                >
                  <Trash2 size={14}/>
                </button>
              </div>
            ))
          )}
        </div>
        </div>

        {/* School Info - Fixed at bottom of sidebar */}
        <div className="p-4 border-t border-slate-100 bg-white/50 shrink-0">
          <div className="flex items-center gap-3 p-2 rounded-xl hover:bg-slate-100/50 transition-all cursor-pointer group">
            <div className="w-10 h-10 bg-linear-to-tr from-slate-100 to-slate-200 rounded-full flex items-center justify-center text-slate-500 group-hover:from-blue-50 group-hover:to-blue-100 group-hover:text-blue-600 transition-all">
              <Building2 size={20}/>
            </div>
            <div className="flex-1 overflow-hidden">
              <div className="text-sm font-bold truncate text-slate-800">Trường CNTT-TT</div>
              <div className="text-[10px] text-slate-400 font-medium">Đại học Cần Thơ</div>
            </div>
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <main className="flex flex-col flex-1 relative min-w-0 bg-white">
        {/* Transparent Header */}
        <header className="h-16 flex items-center justify-between px-8 bg-white/80 backdrop-blur-md border-b border-slate-100 sticky top-0 z-20">
          <div className="flex items-center gap-4">
            <button 
              onClick={() => setIsSidebarOpen(!isSidebarOpen)}
              className="md:hidden p-2 text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-all"
            >
              <MessageSquare size={20}/>
            </button>
            <div className="flex items-center gap-2 px-3 py-1 bg-emerald-50 rounded-full border border-emerald-100">
              <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse"></div>
              <span className="text-[11px] font-bold text-emerald-700 uppercase tracking-tight">Hệ thống đang trực tuyến</span>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <button className="p-2 text-slate-400 hover:text-slate-600 hover:bg-slate-100 rounded-lg transition-all"><Share2 size={18}/></button>
            <div className="h-4 w-px bg-slate-200 mx-1"></div>
            <button className="p-2 text-slate-400 hover:text-slate-900 transition-colors"><Github size={20}/></button>
          </div>
        </header>

        {/* Mobile Sidebar Overlay */}
        {isSidebarOpen && (
          <div 
            className="fixed inset-0 bg-black/20 z-30 md:hidden"
            onClick={() => setIsSidebarOpen(false)}
          />
        )}

        {/* Conversation View */}
        <div ref={scrollRef} className="flex-1 overflow-y-auto">
          <div className="max-w-3xl mx-auto w-full py-10 px-6 space-y-10">
            {messages.length === 0 ? (
              <div className="py-20 flex flex-col items-center justify-center text-center space-y-6">
                <div className="w-20 h-20 bg-blue-50 rounded-4xl flex items-center justify-center text-blue-600 rotate-3 hover:rotate-0 transition-transform duration-500 shadow-inner">
                  <Bot size={40}/>
                </div>
                <div className="space-y-2">
                  <h2 className="text-3xl font-extrabold text-slate-800 tracking-tight">CICTBot</h2>
                  <p className="text-slate-500 max-w-sm mx-auto text-sm leading-relaxed">
                    Tôi là trợ lý ảo được huấn luyện để trích xuất và trả lời thông tin văn bản cho Văn phòng Trường CNTT-TT.
                  </p>
                </div>
                <div className="flex flex-wrap justify-center gap-2 pt-2">
                  {["Môn tiên quyết là gì?", "Cấu trúc ngành CNTT?", "Điều kiện tốt nghiệp?"].map(q => (
                    <button key={q} onClick={() => setInput(q)} className="px-3 py-1.5 bg-white border border-slate-200 rounded-full text-xs font-medium text-slate-600 hover:border-blue-400 hover:text-blue-600 hover:shadow-sm transition-all">
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            ) : (
              messages.map(m => (
                <div key={m.id} className={`flex gap-4 ${m.role === "user" ? "flex-row-reverse" : "flex-row"} group animate-in fade-in slide-in-from-bottom-2 duration-300`}>
                  <div className={`w-9 h-9 rounded-xl flex items-center justify-center shrink-0 ${
                    m.role === "user" 
                      ? "bg-blue-600 text-white" 
                      : "bg-white border border-slate-200 text-emerald-600"
                  }`}>
                    {m.role === "user" ? <User size={18}/> : <Bot size={18}/>}
                  </div>
                  
                  <div className={`flex flex-col gap-2 max-w-[85%] ${m.role === "user" ? "items-end" : "items-start"}`}>
                    <div className={`px-4 py-2.5 rounded-2xl text-[15px] leading-6 transition-all ${
                      m.role === "user" 
                        ? "bg-blue-600 text-white rounded-tr-xl" 
                        : "bg-white border border-slate-200 text-white rounded-tl-xl"
                    }`}>
                      <article className={`prose prose-sm max-w-none ${m.role === 'user' ? 'prose-invert' : 'prose-slate'}`}>
                        <div className="whitespace-pre-wrap break-words">
                          <ReactMarkdown remarkPlugins={[remarkGfm]}>
                            {m.parts?.[0]?.text?.replace(/\n{3,}/g, '\n\n').trim()}
                          </ReactMarkdown>
                        </div>
                      </article>
                    </div>
                    
                    {/* Bot Actions Area */}
                    {m.role === "assistant" && m.parts[0].text && (
                      <div className="flex items-center gap-1 ml-1 opacity-0 group-hover:opacity-100 transition-opacity">
                        <button className="p-1.5 text-slate-400 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-all"><Copy size={14}/></button>
                        <button className="p-1.5 text-slate-400 hover:text-emerald-600 hover:bg-emerald-50 rounded-md transition-all"><ThumbsUp size={14}/></button>
                        <button className="p-1.5 text-slate-400 hover:text-orange-600 hover:bg-orange-50 rounded-md transition-all"><RefreshCw size={14}/></button>
                      </div>
                    )}
                  </div>
                </div>
              ))
            )}

            {status === "streaming" && (
              <div className="flex gap-4 items-start">
                <div className="w-9 h-9 rounded-xl bg-white border border-slate-200 text-emerald-500 flex items-center justify-center">
                  <Bot size={18}/>
                </div>
                <div className="bg-slate-50 border border-slate-200 px-4 py-2.5 rounded-2xl rounded-tl-xl text-slate-500 text-[15px] flex items-center gap-2">
                  <div className="flex gap-1">
                    <span className="w-1.5 h-1.5 bg-slate-400 rounded-full animate-bounce [animation-delay:-0.3s]"></span>
                    <span className="w-1.5 h-1.5 bg-slate-400 rounded-full animate-bounce [animation-delay:-0.15s]"></span>
                    <span className="w-1.5 h-1.5 bg-slate-400 rounded-full animate-bounce"></span>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Floating Input Bar */}
        <footer className="p-6 md:p-10 bg-linear-to-t from-white via-white to-transparent">
          <form onSubmit={handleSend} className="max-w-3xl mx-auto">
            <div className="relative group shadow-2xl shadow-slate-200 rounded-4xl overflow-hidden border border-slate-200 focus-within:border-blue-400 focus-within:ring-4 focus-within:ring-blue-50 transition-all bg-white p-2">
              <div className="flex items-end">
                <textarea
                  rows={1}
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault()
                      handleSend(e as any)
                    }
                  }}  
                  placeholder="Hỏi bất cứ điều gì về học vụ CTU..."
                  className="w-full resize-none p-3 pl-4 bg-transparent text-slate-800 outline-none text-[15px] min-h-13 max-h-48 scrollbar-hide"
                />
                <button
                  type="submit"
                  disabled={status === "streaming" || !input.trim()}
                  className={`mb-1 mr-1 p-3 rounded-2xl transition-all flex items-center justify-center ${
                    status === "streaming" || !input.trim()
                      ? "bg-slate-50 text-slate-300"
                      : "bg-blue-600 text-white hover:bg-blue-700 shadow-lg shadow-blue-200 active:scale-95"
                  }`}
                >
                  {status === "streaming" ? <Loader2 className="animate-spin w-5 h-5"/> : <Send size={20} className="ml-0.5"/>}
                </button>
              </div>
            </div>
            <p className="text-[10px] text-center text-slate-400 mt-4 font-medium opacity-60">
              Chatbot có thể mắc lỗi. Vui lòng xác nhận thông tin quan trọng từ nguồn chính thức.
            </p>
          </form>
        </footer>
      </main>
    </div>
  )
}